import hashlib
import datetime
import random
import string
import os 
import tarfile
import shlex
import json
import time
import select
from hashlib import sha1
try:
    # python2
    import cStringIO
    BytesIO = cStringIO.StringIO
except:
    # python3
    import io
    BytesIO = io.BytesIO

from metaflow.runtime import MAX_WORKERS,MAX_WORKERS,MAX_LOG_SIZE,PROGRESS_INTERVAL,MAX_NUM_SPLITS
from metaflow.includefile import InternalFile
from metaflow.datastore.datastore import TransformableObject
from metaflow import util
from metaflow.plugins.kubernetes.kube_client import KubeClient,KubeJobException
from metaflow.plugins.kubernetes.kube import KubeException
from metaflow.metaflow_config import BATCH_METADATA_SERVICE_URL, DATATOOLS_S3ROOT, \
    DATASTORE_LOCAL_DIR, DATASTORE_SYSROOT_S3, DEFAULT_METADATA, \
    BATCH_METADATA_SERVICE_HEADERS, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_DEFAULT_REGION,KUBE_NAMESPACE
from . import runtime_util

LETTER_CHOICES = string.ascii_letters+string.digits

class KubeDeployRuntime(object):
    """KubeDeployRuntime 
    This is a class dedicated to deploying a packaged Metaflow Runtime container that will orchestrate the DAG using Kubernetes Jobs. 
    Key Methods : 
        - __init__ : Sets the main properties of the Object. 
        - persist_runtime : Needs to be explicitly called. It will ensure that file based data artifacts, parameters, code package is persisted for retrieval on container.  
        - deploy : This method will spawn a KubeJob object which will take the set necessary parameters to deploy to Kubernetes
    Key Assumptions : 
        - Conda is needed to isolate the environment of the step. So deploying runtime in an miniconda3 docker image satisfies the dependency management needed for working with conda.
        - The runtime is only orchestrating tasks and syncing when to run the next task. 
            - If dependencies/environment for the tasks are isolated by the docker image provided in @kube / --with then the runtime doesn't require any more deps than given in the `get_package_commands`
        - inherits environment variables like METAFLOW_KUBE_NAMESAPCE for containers running with same Namespace.
    Key Constraint : 
        - Coupled with S3. Not so thightly coupled but coupled in the setup process of the containers. 
        - Ideally provide python:x.y as image in `--with` when running with --environment=conda
    """
    deployment_store_prefix = '/kube_deployment/'
    include_data_store = '/include_artifacts/'
    job_type = 'runtime_execution'
    package_url = None
    package_sha = None

    def __init__(self, # Start with supporting only deploy run. Resume can come later with run_id support. 
                 flow,
                 graph,
                 datastore,
                 metadata,
                 environment,
                 package,
                 logger,
                 entrypoint,
                 event_logger,
                 monitor,
                 max_workers=MAX_WORKERS,
                 max_num_splits=MAX_NUM_SPLITS,
                 max_log_size=MAX_LOG_SIZE,
                 kube_namespace=None,
                 partial_runtime_cli=None,
                 max_runtime_cpu=None,
                 max_runtime_memory=None,
                 **kwargs):
        super().__init__()
        
        self._client = KubeClient()
        
        self._flow = flow
        self._graph = graph
        self._datastore = datastore
        self._metadata = metadata
        self._package = package
        self._environment = environment
        self._logger = logger
        self._entrypoint = entrypoint
        self.event_logger = event_logger
        self._monitor = monitor
        self._kube_namespace = KUBE_NAMESPACE if kube_namespace is None else kube_namespace 
        self._final_cli = partial_runtime_cli # This will not be partial.
        # $ Set new Datastore Root here for saving data related to this deployment here. 
        self.max_runtime_cpu= max_runtime_cpu
        self.max_runtime_memory= max_runtime_memory
        self._datastore.datastore_root = self._datastore.datastore_root+self.deployment_store_prefix 
        self._ds = None
        self.deployment_id = None
        env_inf = self._environment.get_environment_info()
        self.docker_image = 'python:'+ env_inf['python_version_code'] # $ todo: support conda. 

        self.needed_env_var = {}
        self._max_workers = max_workers
        self._max_num_splits = max_num_splits
        self._max_log_size = max_log_size
        
        # this will be used for syncing the files in the right other to S3. 
        self._internal_syncing_files = []
        self.include_artifact_url = None

        self._supplied_kwargs = kwargs
        self._send_kwargs = {}
        
        # collect files for syncing so that they can be placed in the datastore and extracted in the container.     

        for arg in kwargs: # This is done so that we have either a path or param to deal with when building CLI args.The _send_kwargs method will have all the data. 
            if isinstance(kwargs[arg], InternalFile):
                self._internal_syncing_files.append({'arg':arg,'file':kwargs[arg]})
                self._send_kwargs[arg] = kwargs[arg]._path
            else:
                self._send_kwargs[arg] = kwargs[arg]
        
    
    def _name_str(self, user, flow_name):
        return '{user}-{flow_name}-{date_str}'.format(
            user=str.lower(user),
            flow_name=str.lower(flow_name),
            date_str=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ) 

    # $ (TODO):Make it Flexible for Other cloud Providers 
    def _datainitialization_commands(self,environment,data_package_url):
        return [
            "echo \'Downloading Data depencies.\'",
            "i=0; while [ $i -le 5 ]; do "
                    "echo \'Downloading Data package.\'; "
                    "%s -m awscli s3 cp %s job_data.tar >/dev/null && echo \'Data package downloaded.\' && break;"
                    "sleep 10; i=$((i+1));"
            "done " % (environment._python(), data_package_url),
            "tar xf job_data.tar"
        ]
    
    # $ This needed to move away from the environment because we need to isolate the runtime. 
    def get_package_commands(self, code_package_url):
        cmds = ["set -e",
                "echo \'Setting up task environment.\'",
                "%s -m pip install awscli click requests boto3 \
                    --user -qqq" % self._python(),
                "mkdir metaflow",
                "cd metaflow",
                "i=0; while [ $i -le 5 ]; do "
                    "echo \'Downloading code package.\'; "
                    "%s -m awscli s3 cp %s job.tar >/dev/null && \
                        echo \'Code package downloaded.\' && break; "
                    "sleep 10; i=$((i+1));"
                "done " % (self._python(), code_package_url),
                "tar xf job.tar"
                ]
        return cmds

    def _python(self):
        return "python"

    # $ no environment related tasks are carried instead an image is used with conda to ensure conda is runnanble.
    # $ In case of metaflow, it tires to scope the environment of each step. We are ensuring that using Conda and Images with `--with`
    # $ So ensuring the Native runtime works, Python supported image is only needed. 
    def bootstrap_environment(self,evnironment):
        if evnironment.TYPE == 'conda':
            self.docker_image = 'continuumio/miniconda3'
            self.needed_env_var['CONDA_CHANNELS'] = 'conda-forge'
            return []
        
        return []
        

    # $ This will Generate the Packaged Environment to Run on Kubernetes --> Packaging is simple. There is either conda or a Docker image. For th runtime it doesn't matter. 
    def _command(self, code_package_url, environment, runtime_cli,data_package_url):
        cmds = self.get_package_commands(code_package_url)
        # $ Added this line because its not present in the batch. 
        cmds.extend(["%s -m pip install kubernetes --user -qqq" % self._python()])
        cmds.extend(self.bootstrap_environment(environment))
        data_init_commands = self._datainitialization_commands(environment,data_package_url)
        cmds.extend(data_init_commands)
        cmds.append("echo 'Task is starting'")
        cmds.extend(runtime_cli)
        return shlex.split('/bin/sh -c "%s"' % " && ".join(cmds))


    def _job_name(self, user, flow_name): # $ Name Generated using MD5 Hash because of Length problems. Labels are used instead. 
        # $ Name can only be as Long as 65 Chars. :: https://stackoverflow.com/questions/50412837/kubernetes-label-name-63-character-limit
        curr_name = self._name_str(user, flow_name)
        # $ SHA the CURR Name and 
        random_bytes_to_name = ''.join([ LETTER_CHOICES[random.randint(0,len(LETTER_CHOICES)-1)] for i in range(0,13)])
        curr_name = str.lower(hashlib.md5(curr_name.encode()).hexdigest()+'-'+random_bytes_to_name)
        return curr_name


    def persist_runtime(self,username):
        """persist_runtime 
        - create a deployment ID 
        - sync dependencies into datastore under the deployment_id
        - Persist Params in a way that they replicate the real param names on the kube runtime.(file path defaults are set here.)
        - save data into the deployment so that it can untared later at time of deployment.
        """
        self.deployment_id = self._job_name(username,self._flow.name)
        # $ set datastore root to deployment_id
        self._datastore.datastore_root = self._datastore.datastore_root + self.deployment_id
        self._ds = self._datastore(self._flow.name,mode='w')
        # $ Create a way to persist the runtime by changing the file based Params to defaults
        self._logger('Persisting Parameters')
        self._persist_params()
        self._logger('Saving Code Packages')
        # $ save packages to extract later. 
        self._save_package_once(self._ds,self._package)
        # print("Package Saved at : ",self.package_url)
        # $ Set data artifact storage to datastore. 
        # $ Syncing Data artifacts in a way that they follow Path defaults. 
        self._logger('Persisting File Artifacts')
        self.include_artifact_url = self._save_artifacts(self._ds)

    def _persist_params(self):
        """_persist_params [summary]
        # This will set the Runtime_cli params Which we not fully set when given the KubeDeployRuntime. 
        # For params which have files which are lying in weird paths such as ../filename or ../../../filename :
        # bring them to one solid place(defaults) and So that when untared they directly set a structure acceptible to doing a run which works. 
        # todo : Add logs for package persistance. 
        """
        input_file_dict,input_param_dict = runtime_util.get_real_param_values()
        runtime_cli_args = None
        arguement_lists = []
        # $ Setting Path defaults here so that files from what ever locations can be added to tar package with defaults being used. 
        for file_parsed_param_name in input_file_dict:
            holding_val = self._send_kwargs[file_parsed_param_name]
            arguement_lists.append(['--'+input_file_dict[file_parsed_param_name]['actual_name'],input_file_dict[file_parsed_param_name]['default_path']])
            self._send_kwargs[file_parsed_param_name] = input_file_dict[file_parsed_param_name]['default_path']
        
        for parsed_param_name in input_param_dict:
            holding_val = self._send_kwargs[parsed_param_name]
            arguement_lists.append(['--'+input_param_dict[parsed_param_name]['actual_name'],holding_val])

        runtime_cli_args = ' '.join([' '.join([str(a) for a in arg1]) for arg1 in arguement_lists])
        # $ Setting final CLI Here. 
        self._final_cli = '{partial_cli} {cli_args}'.format(partial_cli=self._final_cli,cli_args=runtime_cli_args)
        # print(self._final_cli)


    def deploy(self,attrs=None,env=None):
        """deploy 
            runs after persist_runtime() so checks for necessary artifacts being present.
            It will create a kubernetes job spec that will run the native runtime inside isolated container. 
            Some conditions : 
                - Explicitly set ENV Vars for : 
                    - METAFLOW_DEFAULT_METADATA : service (Inside Kubecluster local metadata provider is not easy to manage.
                                                        SO currently only service based support.)
                    
                    - METAFLOW_DEFAULT_DATASTORE : As the Datastore allowed via CLI

                    - METAFLOW_KUBE_NAMESAPCE : This will ensure correct namespace of runtime and its subsequent job deployments. 

                    - METAFLOW_KUBE_RUNTIME_IN_CLUSTER : 'yes' --> So that containers can be spawned for steps by runtime from within a container in the cluster 
                
                - Explicitly set meta_data_label
                    
                    - job_type : This is to differentiate between subjobs and runtime jobs running on kube. 

                - Setting static docker image because we want to replicate the runtime on a container.
                to ideally run a Nativeruntime one should have a Python:version_num based image.             
        """
        
        kube_job = self._client.job()
        kube_job \
                .job_name(self.deployment_id) \
                .command(
                self._command(self.package_url,
                              self._environment, [self._final_cli],self.include_artifact_url)) \
                .environment_variable('METAFLOW_CODE_SHA', self.package_sha) \
                .environment_variable('METAFLOW_CODE_URL', self.package_url) \
                .environment_variable('METAFLOW_CODE_DS', self._datastore.TYPE) \
                .environment_variable('METAFLOW_USER', attrs['metaflow.user']) \
                .environment_variable('METAFLOW_SERVICE_URL', BATCH_METADATA_SERVICE_URL) \
                .environment_variable('METAFLOW_DEFAULT_METADATA','service') \
                .environment_variable('METAFLOW_SERVICE_HEADERS', json.dumps(BATCH_METADATA_SERVICE_HEADERS)) \
                .environment_variable('METAFLOW_DATASTORE_SYSROOT_S3', DATASTORE_SYSROOT_S3) \
                .environment_variable('METAFLOW_DATATOOLS_S3ROOT', DATATOOLS_S3ROOT) \
                .environment_variable('METAFLOW_DEFAULT_DATASTORE', self._datastore.TYPE) \
                .environment_variable('AWS_ACCESS_KEY_ID', AWS_ACCESS_KEY_ID) \
                .environment_variable('AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY) \
                .environment_variable('AWS_SESSION_TOKEN', AWS_SESSION_TOKEN) \
                .environment_variable('AWS_DEFAULT_REGION', AWS_DEFAULT_REGION) \
                .environment_variable('METAFLOW_KUBE_NAMESAPCE',self._kube_namespace) \
                .environment_variable('METAFLOW_KUBE_RUNTIME_IN_CLUSTER','yes') \
                .image(self.docker_image) \
                .max_cpu(self.max_runtime_cpu) \
                .max_memory(self.max_runtime_memory) \
                .meta_data_label('user', attrs['metaflow.user']) \
                .meta_data_label('flow_name', attrs['metaflow.flow_name']) \
                .meta_data_label('job_type',self.job_type) \
                .namespace(self._kube_namespace) 
                # $ (TODO) : Set the AWS Keys based Kube Secret references here.
        
        self._logger(head='Deploying Metaflow Runtime on Kuberenetes')
        for name, value in env.items():
            kube_job.environment_variable(name, value)
        for name,value in self.needed_env_var.items():
            kube_job.environment_variable(name, value)

        for name, value in self._metadata.get_runtime_environment('kube').items():
            kube_job.environment_variable(name, value)
        if attrs:
            for key, value in attrs.items():
                kube_job.parameter(key, value)
        
        self.job = kube_job.execute()
        
    def _save_artifacts(self,datastore):
        """_save_artifacts [summary]
        self._send_kwargs holds the cli arguments set for the run. 
        For the files they are the default paths that are provided. 
        So we will Tar the archieve with `arcname` to set file structure mentioned in defaults of each file based param
        We are doing this because this ensures that the run will never break even if the input files is coming present on what ever path.
        """
        # with util.TempDir() as data_store_sync_dir:            
        with util.TempDir() as td:
            tar_file_path = os.path.join(td, 'include_artifacts.tgz')
            buf = BytesIO()
            with tarfile.TarFile(fileobj=buf, mode='w') as tar:
                for internal_file_json in self._internal_syncing_files:
                    # $ This will use the `arcname` to set the Deployment's data in archieve with file structure used by default values. 
                    tar.add(internal_file_json['file']._path,arcname=self._send_kwargs[internal_file_json['arg']])

            blob = buf.getvalue()
            url = datastore.save_data('include_artifacts.tgz',TransformableObject(blob))
            return url    
            

    def wait_to_complete(self,echo=None):
        def wait_for_launch(job,logger):
            status = job.status
            echo(job.id,'Task is starting (status %s)...' % status)
            t = time.time()
            while True:
                if status != job.status or (time.time()-t) > 30:
                    status = job.status
                    echo(job.id,'Task is starting (status %s)...' % status)
                    # logger(body=util.to_unicode('Task is starting (status %s)...' % status),head=job.id)
                    # logger('[{job_id}] {line}'.format(job_id=job.id,line=util.to_unicode('Task is starting (status %s)...' % status)),head=True)
                    t = time.time()
                if self.job.is_running or self.job.is_done or self.job.is_crashed:
                    break
                select.poll().poll(200)

        def print_all(tail):
            for line in tail:
                if line:
                    echo(self.job.id,line)
                    # self._logger(body=util.to_unicode(line),head=self.job.id)
                    # self._logger('[{job_id}] {line}'.format(job_id=self.job.id,line=util.to_unicode(line))) 
                else:
                    return tail, False
            return tail, True

        wait_for_launch(self.job,self._logger)
        
        # $ (TODO) : This may have issues. Check this During Time of Execution.
        logs = self.job.logs()

        while True:
            logs, finished, = print_all(logs)
            if finished:
                break
            else:
                select.poll().poll(500)

        while True:
            if self.job.is_done or self.job.is_crashed:
                select.poll().poll(500)
                break

        if self.job.is_crashed:
            if self.job.reason:
                raise KubeException(
                    'Task crashed due to %s .'
                    'This could be a transient error. '
                    'Use @retry to retry.' % self.job.reason
                )
            raise KubeException(
                'Task crashed. '
                'This could be a transient error. '
                'Use @retry to retry.'
            )
        else:
            if self.job.is_running:
                # Kill the job if it is still running by throwing an exception.
                raise KubeException("Task failed!")
            
            echo(self.job.id,'Task Finshed with (status %s)...' % self.job.status)
            # self._logger(body=util.to_unicode('Task Finshed with (status %s)...' % self.job.status),head=self.job.id)
            # self._logger('[{job_id}] {line}'.format(job_id=self.job.id,line=util.to_unicode('Task Finshed with (status %s)...' % self.job.status)),head=True)

    

    def list_jobs(self,flow_name,username,echo):
        jobs = self._search_jobs(flow_name, username)
        if jobs:
            for job in jobs:
                job_name = self._name_str(job.labels['user'],job.labels['flow_name'])+'-'+self.job_type
                echo(
                    '{name} [{id}] ({status})'.format(
                        name=job_name, id=job.id, status=job.status
                    )
                )
        else:
            echo('No running Kube jobs found.')


    def _search_jobs(self, flow_name, user): # $ The Function Works.
        """_search_jobs [Searches the jobs on Kubernetes. These will be runtime_execution jobs.]
        :rtype: [List[KubeJobSpec]]
        """
        # todo : throw error if there is no flow name
        search_object = {'flow_name': flow_name,'job_type':'runtime_execution'}
        if user is not None:
            search_object['user'] = user
        jobs = []
        for job in self._client.unfinished_jobs():
            # $ Use Labels to indentify jobs and thier executions. 
            job_labels = dict(job.labels)
            if set(job_labels.items()).intersection(set(search_object.items())) == set(search_object.items()):
                jobs.append(job)

        return jobs

    # Not a class method because the scope is only set once. 
    @classmethod
    def _save_package_once(cls,datastore,package):
        if cls.package_url is None:
            cls.package_url = datastore.save_data(package.sha, TransformableObject(package.blob))
            cls.package_sha = package.sha

    