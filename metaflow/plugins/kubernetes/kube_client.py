from collections import defaultdict, deque
import select
import sys
import time
import hashlib

try:
    unicode
except NameError:
    unicode = str
    basestring = str

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import get_kubernetes_client

import kubernetes.client as kube_client
from kubernetes import watch
from kubernetes.client.rest import ApiException

MAX_MEMORY = 32*1000
MAX_CPU = 8

    
class KubeClient(object):
    def __init__(self):
        # todo : set the 
        self._client = get_kubernetes_client()

    def unfinished_jobs(self):
        """unfinished_jobs [Gets the Kube jobs which are unfinished.]
        
        :return: [tuple with KubeJobSpec Objects]
        :rtype: [Tuple(KubeJobSpec)]
        """
        from metaflow.client import get_namespace
        # $ FIGURE KUBE API To Check and Find whichever Jobs are Active,
        jobs = kube_client.BatchV1Api(self._client).list_namespaced_job(get_namespace(),include_uninitialized=False,timeout_seconds=60)
        return (KubeJobSpec(self._client,job.metadata.name,job.metadata.namespace) for job in jobs.items if job.status.active is not None)

    def job(self):
        return KubeJob(self._client)

    def attach_job(self, job_name,namespace):
        job = RunningKubeJob(self._client,job_name,namespace)
        return job.update()


class KubeJobException(MetaflowException):
    headline = 'Kube job error'


class KubeJob(object):
    def __init__(self, client):
        self._client = client
        self._api_client = kube_client.BatchV1Api(client)
        self.payload = kube_client.V1Job(api_version="batch/v1", kind="Job")
        self.payload.metadata = kube_client.V1ObjectMeta()
        self.payload.status = kube_client.V1JobStatus()
        self.namespace_name = None
        self.name = None
        self.template = kube_client.V1PodTemplate()
        self.template.tempate = kube_client.V1PodTemplateSpec()
        self.template.tempate.spec = client.V1PodSpec()
        self.env_list = []
        self._image = None
        self.container = kube_client.V1Container(name='metaflow-job') 
        self.command = None # $ Need to figure how to structure this properly. 
        self.container.resources.limits.cpu = MAX_CPU*1000+"m" # $ NOTE: Currently Setting Hard Limits. Will Change Later
        self.container.resources.limits.memory = MAX_MEMORY+"Mi" # $ NOTE: Currently Setting Hard Limits. Will Change Later


    def execute(self):
        """execute [Runs the Job and yields a RunningKubeJob object]
        :raises KubeJobException: [Upon failure on execution]
        :return: RunningKubeJob
        :rtype: [RunningKubeJob]
        """
        if self._image is None:
            raise KubeJobException(
                'Unable to launch Kubernetes Job job. No docker image specified.'
            )
        if self.namespace_name is None:
            raise KubeJobException("Unable to launch Kubernetes Job Without Namespace.")

        self.container.image = self._image
        self.container.env = self.env_list
        self.template.template.spec.containers = [self.container]
        self.payload.spec = kube_client.V1JobSpec(ttl_seconds_after_finished=600, template=self.template.template)
        try: 
            api_response = self._api_client.create_namespaced_job(self.namespace,self.payload)
            job_id = api_response['metadata']['uid']
        except ApiException as e:
            KubeJobException("Exception when calling API: %s\n" % e)

        
        # $ TODO :  Return the Job From here. 
        job = RunningKubeJob(self._client,self.name,self.namespace)
        return job.update()

    def job_name(self, job_name):
        self.payload.metadata.name = job_name
        self.name = job_name
        return self

    def namespace(self,namespace_name):
        self.namespace_name = namespace_name
        return self

    def image(self, image):
        self._image = image
        return self

    def args(self,args):
        if not isinstance(args,list) :
            raise KubeJobException("Invalid Args Type. Needs to be Of Type List but got {}".format(type(args)))
        self.container.args = args
        return self

    def command(self, command):
        if not isinstance(command,list) :
            raise KubeJobException("Invalid Command Type. Needs to be Of Type List but got {}".format(type(command)))
        self.container.command = command
        return self

    def cpu(self, cpu):
        if not (isinstance(cpu, (int, unicode, basestring)) and int(cpu) > 0):
            raise KubeJobException(
                'Invalid CPU value ({}); it should be greater than 0'.format(cpu))
        
        self.container.resources.requests.cpu = int(cpu)*1000+"m"
        return self

    def memory(self, mem):
        if not (isinstance(mem, (int, unicode, basestring)) and int(mem) > 0):
            raise KubeJobException(
                'Invalid memory value ({}); it should be greater than 0'.format(mem))
        self.container.resources.requests.memory = int(mem)+"Mi"
        return self

    # THIS WILL COME LATER
    def gpu(self, gpu):
        if not (isinstance(gpu, (int, unicode, basestring))):
            raise KubeJobException(
                'invalid gpu value: ({}) (should be 0 or greater)'.format(gpu))
        if int(gpu) > 0:
            pass # $ todo : Figure GPU Here. 
        return self

    def environment_variable(self, name, value):
        self.env_list.append(kube_client.V1EnvVar(name=name,value=value))
        return self

    def timeout_in_secs(self, timeout_in_secs):
        self.payload['timeout']['attemptDurationSeconds'] = timeout_in_secs
        return self


class limit(object):
    def __init__(self, delta_in_secs):
        self.delta_in_secs = delta_in_secs
        self._now = None

    def __call__(self, func):
        def wrapped(*args, **kwargs):
            now = time.time()
            if self._now is None or (now - self._now > self.delta_in_secs):
                func(*args, **kwargs)
                self._now = now
        return wrapped


class KubeJobSpec(object):
    def __init__(self,client,job_name,namespace):
        super().__init__()
        self._client = client
        self._batch_api_client = kube_client.BatchV1Api(client)
        self.job_name = job_name
        self.namespace = namespace
        self._data = {}
    def __repr__(self):
        return '{}(\'{}\')'.format(self.__class__.__name__, self._id)

    def _apply(self, data):
        self._data = data

    @limit(1)
    def _update(self):
        try:
            # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/BatchV1Api.md#read_namespaced_job
            data = self._batch_api_client.read_namespaced_job(self.job_name,self.namespace)
        except ApiException as e :
            return
        self._apply(data)

    def update(self):
        self._update()
        return self
    
    @property
    def id(self):
        return self.info.metadata.uid

    @property
    def info(self):
        if not self._data:
            self.update()
        return self._data

    @property
    def job_name(self):
        return self.info.metadata.name

    @property
    def status(self):
        if not self.is_done:
            self.update()
        if self.is_running:
            return 'RUNNING'
        elif self.is_successful:
            return 'COMPLETED'
        elif self.is_crashed:
            return 'FAILED'
        else:
            return 'UNKNOWN_STATUS'
        

    @property
    def status_reason(self):
        return self.reason

    @property
    def created_at(self):
        return self.info.status.start_time

    @property
    def is_done(self):
        if self.info.status.completion_time is None:
            self.update()
        return self.info.status.completion_time is None

    @property
    def is_running(self):
        return self.info.status.active == 1

    @property
    def is_successful(self):
        return self.info.status.succeeded is not None

    @property
    def is_crashed(self):
        # TODO: Check statusmessage to find if the job crashed instead of failing
        return self.info.status.failed is not None

    @property
    def reason(self):
        reason = []
        if self.info.status.conditions is not None:
            for obj in self.info.status.conditions:
                if obj.reason is not None:
                    reason.append(obj.reason)

        return '\n'.join(reason)


# $ ? Doubt : Why do u inherit Object ?
class RunningKubeJob(KubeJobSpec):

    NUM_RETRIES = 5

    def __init__(self, client, job_name, namespace):
        super().__init__(client, job_name, namespace)

    # $ https://stackoverflow.com/questions/56124320/how-to-get-log-and-describe-of-pods-in-kubernetes-by-python-client
    def logs(self):
        pod_label_selector = "controller-uid=" + self.info.metadata.labels.get('controller-uid')
        pods_list = kube_client.CoreV1Api(self._client).list_namespaced_pod(self.namespace,label_selector=pod_label_selector, timeout_seconds=10)
        pod_name = pods_list.items[0].metadata.name
        # There is no Need to check if the job is in runnable state as the Job will be runnnig on Kube
        watcher = watch.Watch()
        for i in range(self.NUM_RETRIES):
            try:
                check_after_done = 0
                for line in watcher.stream(kube_client.CoreV1Api(self._client).read_namespaced_pod_log, name=pod_name, namespace=self.namespace):
                    if not line:
                        if self.is_done:
                            if check_after_done > 1:
                                return
                            check_after_done += 1
                        else:
                            pass
                    else:
                        yield line
            except Exception as ex:
                if self.is_crashed:
                    break
                sys.stderr.write(repr(ex))
                time.sleep(2 ** i)

    def kill(self):
        if not self.is_done:
            self._batch_api_client.delete_namespaced_job(self.job_name,self.namespace)
        return self.update()
