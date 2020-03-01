import hashlib
import datetime
import random
import string
import os 
import tarfile
import shlex
import json

from metaflow.runtime import MAX_WORKERS,MAX_WORKERS,MAX_LOG_SIZE,PROGRESS_INTERVAL,MAX_NUM_SPLITS
from metaflow.includefile import InternalFile
from metaflow.datastore.datastore import TransformableObject
from metaflow import util
from metaflow.plugins.kubernetes.kube_client import KubeClient
from metaflow.metaflow_config import BATCH_METADATA_SERVICE_URL, DATATOOLS_S3ROOT, \
    DATASTORE_LOCAL_DIR, DATASTORE_SYSROOT_S3, DEFAULT_METADATA, \
    BATCH_METADATA_SERVICE_HEADERS, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_DEFAULT_REGION,KUBE_NAMESPACE


LETTER_CHOICES = string.ascii_letters+string.digits

class KubeDeployRuntime(object):

    deployment_store_prefix = '/kube_deployment/'
    include_data_store = '/include_artifacts/'
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
                 dont_exit=False,
                 kube_namespace=None,
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
        if kube_namespace is None:
            self._kube_namespace = kube_namespace
        # $ Set new Datastore Root here for saving data related to this deployment here. 
        
        self._datastore.datastore_root = self._datastore.datastore_root+self.deployment_store_prefix 
        self._ds = None
        self.deployment_id = None

        self._max_workers = max_workers
        self._max_num_splits = max_num_splits
        self._max_log_size = max_log_size
        
        # this will be used for syncing the files in the right other to S3. 
        self._internal_syncing_files = []
        self.include_artifact_url = None

        self._supplied_kwargs = kwargs
        self._send_kwargs = {}
        # collect files for syncing so that they can be placed in the datastore and extracted in the container.     
        for arg in kwargs:
            if isinstance(kwargs[arg], InternalFile):
                self._internal_syncing_files.append(kwargs[arg])
                self._send_kwargs[arg] = kwargs[arg]._path
                # print(kwargs[arg]._path)
            else:
                self._send_kwargs[arg] = kwargs[arg]
    
    def _name_str(self, user, flow_name):
        return '{user}-{flow_name}-{date_str}'.format(
            user=str.lower(user),
            flow_name=str.lower(flow_name),
            date_str=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ) 

    def _datainitialization_commands(self,environment,data_package_url):
        return [
            "echo \'Downloading Data depencies.\'; ",
            "i=0; while [ $i -le 5 ]; do "
                    "echo \'Downloading code package.\'; "
                    "%s -m awscli s3 cp %s job.tar >/dev/null && \
                        echo \'Code package downloaded.\' && break; "
                    "sleep 10; i=$((i+1));"
            "done " % (environment._python(), data_package_url),
            "tar xf job.tar"
        ]

    # $ This will Generate the Packaged Environment to Run on Kubernetes
    def _command(self, code_package_url, environment, runtime_cli):
        cmds = environment.get_package_commands(code_package_url)
        # $ Added this line because its not present in the batch. 
        cmds.extend(["%s -m pip install kubernetes \
                    --user -qqq" % environment._python()])
        cmds.extend(environment.bootstrap_commands('start'))
        cmds.extend(self._datainitialization_commands(environment))
        cmds.append("echo 'Task is starting.'")
        cmds.extend(runtime_cli)

        return shlex.split('/bin/sh -c "%s"' % " && ".join(cmds))


    def _job_name(self, user, flow_name): # $ Name Generated using MD5 Hash because of Length problems. Labels are used instead. 
        # $ Name can only be as Long as 65 Chars. :: https://stackoverflow.com/questions/50412837/kubernetes-label-name-63-character-limit
        curr_name = self._name_str(user, flow_name)
        # $ SHA the CURR Name and 
        random_bytes_to_name = ''.join([ LETTER_CHOICES[random.randint(0,len(LETTER_CHOICES)-1)] for i in range(0,13)])
        curr_name = hashlib.md5(curr_name.encode()).hexdigest()+'-'+random_bytes_to_name
        return curr_name


    def persist_runtime(self,username):
        """initiate 
        - create a deployment ID 
        - sync dependencies into datastore under the deployment_id
        - save data into the deployment so that it can untared later at time of deployment.
        """
        self.deployment_id = self._job_name(username,self._flow.name)
        # $ set datastore root to deployment_id

        self._datastore.datastore_root = self._datastore.datastore_root + self.deployment_id

        self._ds = self._datastore(self._flow.name,mode='w')
        # $ save packages to extract later. 
        self._save_package_once(self._ds,self._package)

        # $ Set data artifact storage to datastore. 
        self.include_artifact_url = self._save_artifacts(self._ds)
        # print(self.package_sha,self.package_url)
        # print(self._ds.datastore_root)
        # print(self._datastore.datastore_root)
        # print(self.include_artifact_url)

    def deploy(self,attrs):
        """deploy 
        runs after persist_runtime()
        It will create a kubernetes job spec that will run the native runtime inside isolated container. 
        """
        kube_job = self._client.job()
        kube_job \
                .job_name(self.deployment_id) \
                .command(
                self._command(self.code_package_url,
                              self.environment, step_cli)) \
                .environment_variable('METAFLOW_CODE_SHA', self.package_sha) \
                .environment_variable('METAFLOW_CODE_URL', self.package_url) \
                .environment_variable('METAFLOW_CODE_DS', self._datastore.TYPE) \
                .environment_variable('METAFLOW_USER', attrs['metaflow.user']) \
                .environment_variable('METAFLOW_SERVICE_URL', BATCH_METADATA_SERVICE_URL) \
                .environment_variable('METAFLOW_SERVICE_HEADERS', json.dumps(BATCH_METADATA_SERVICE_HEADERS)) \
                .environment_variable('METAFLOW_DATASTORE_SYSROOT_S3', DATASTORE_SYSROOT_S3) \
                .environment_variable('METAFLOW_DATATOOLS_S3ROOT', DATATOOLS_S3ROOT) \
                .environment_variable('METAFLOW_DEFAULT_DATASTORE', self._datastore.TYPE) \
                .environment_variable('AWS_ACCESS_KEY_ID', AWS_ACCESS_KEY_ID) \
                .environment_variable('AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY) \
                .environment_variable('AWS_SESSION_TOKEN', AWS_SESSION_TOKEN) \
                .environment_variable('AWS_DEFAULT_REGION', AWS_DEFAULT_REGION) \
                .meta_data_label('user', attrs['metaflow.user']) \
                .meta_data_label('flow_name', attrs['metaflow.flow_name']) \
                .meta_data_label('job_type','runtime_execution') \
                .namespace(kube_namespace) 
                # $ (TODO) : Set the AWS Keys based Kube Secret references here.
                
        for name, value in env.items():
            job.environment_variable(name, value)
        for name, value in self.metadata.get_runtime_environment('kube').items():
            job.environment_variable(name, value)
        if attrs:
            for key, value in attrs.items():
                job.parameter(key, value)



    
    def _save_artifacts(self,datastore):
        with util.TempDir() as td:
            tar_file_path = os.path.join(td, 'include_artifacts.tgz')
            with tarfile.open(tar_file_path, 'w:gz') as tar:
                print("tar_file_path",tar_file_path)
                for internal_file in self._internal_syncing_files:
                    tar.add(internal_file._path)
            with open(tar_file_path, 'rb') as f:
                url = datastore.save_data('include_artifacts.tgz',TransformableObject(f.read()))
            return url    
            


    # Not a class method because the scope is only set once. 
    @classmethod
    def _save_package_once(cls,datastore,package):
        if cls.package_url is None:
            cls.package_url = datastore.save_data(package.sha, TransformableObject(package.blob))
            cls.package_sha = package.sha

    