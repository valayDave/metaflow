import hashlib
import json
import os
import random
import re
import time
import typing
from collections import defaultdict
from datetime import datetime, timedelta


class KubernetesProviderNotFound(Exception):
    headline = "Kubernetes provider not found"


class AirflowSensorNotFound(Exception):
    headline = "Sensor package not found"


LABEL_VALUE_REGEX = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9\-\_\.]{0,61}[a-zA-Z0-9])?$")

TASK_ID_XCOM_KEY = "metaflow_task_id"
RUN_ID_LEN = 12
TASK_ID_LEN = 8

# AIRFLOW_TASK_ID_TEMPLATE_VALUE will work for linear/branched workflows.
# ti.task_id is the stepname in metaflow code.
# AIRFLOW_TASK_ID_TEMPLATE_VALUE uses a jinja filter called `task_id_creator` which helps
# concatenate the string using a `/`. Since run-id will keep changing and stepname will be
# the same task id will change. Since airflow doesn't encourage dynamic rewriting of dags
# we can rename steps in a foreach with indexes (eg. `stepname-$index`) to create those steps.
# Hence : Foreachs will require some special form of plumbing.
# https://stackoverflow.com/questions/62962386/can-an-airflow-task-dynamically-generate-a-dag-at-runtime
# TODO: Reference RUN_ID_PREFIX here
AIRFLOW_TASK_ID_TEMPLATE_VALUE = (
    "airflow-{{ [run_id, ti.task_id, dag_run.dag_id ] | task_id_creator  }}"
)


class SensorNames:
    EXTERNAL_TASK_SENSOR = "ExternalTaskSensor"
    SQL_SENSOR = "SQLSensor"
    S3_SENSOR = "S3KeySensor"

    @classmethod
    def get_supported_sensors(cls):
        return list(cls.__dict__.values())


# TODO: This can be removed. See how labels/annotations are handled in Metaflow today.
def sanitize_label_value(val):
    # Label sanitization: if the value can be used as is, return it as is.
    # If it can't, sanitize and add a suffix based on hash of the original
    # value, replace invalid chars and truncate.
    #
    # The idea here is that even if there are non-allowed chars in the same
    # position, this function will likely return distinct values, so you can
    # still filter on those. For example, "alice$" and "alice&" will be
    # sanitized into different values "alice_b3f201" and "alice_2a6f13".
    if val == "" or LABEL_VALUE_REGEX.match(val):
        return val
    hash = hashlib.sha256(val.encode("utf-8")).hexdigest()

    # Replace invalid chars with dots, and if the first char is
    # non-alphahanumeric, replace it with 'u' to make it valid
    sanitized_val = re.sub("^[^A-Z0-9a-z]", "u", re.sub(r"[^A-Za-z0-9.\-_]", "_", val))
    return sanitized_val[:57] + "-" + hash[:5]


def hasher(my_value):
    return hashlib.md5(my_value.encode("utf-8")).hexdigest()[:RUN_ID_LEN]


def task_id_creator(lst):
    # This is a filter which creates a hash of the run_id/step_name string.
    # Since run_ids in airflow are constants, they don't create an issue with the
    #
    return hashlib.md5("/".join(lst).encode("utf-8")).hexdigest()[:TASK_ID_LEN]


def json_dump(val):
    return json.dumps(val)


def dash_connect(val):
    return "-".join(val)


###=============================SENSOR-METADATA-EXTRACTION===================
# This code helps extract metadata about what happened with sensors.
# It is not being used at the moment but can be very easily in the future.
# Just set `PARENT_TASK_INSTANCE_STATUS_MACRO` to some environment variable
# it will create a json dictionary at task runtime.
# just SensorMetadata? Also, looking at the usage, we should be able to do away with
# this class entirely and embed the logic in get_sensor_metadata?
class SensorMetaExtractor:
    """
    Extracts the metadata about the upstream sensors that are triggering the DAG.
    Stores different metadata information based on different sensor type.
    """

    def __init__(self, task_instance, airflow_operator, dag_run):
        self._data = dict(
            task_id=airflow_operator.task_id,
            state=task_instance.state,
            operator_type=task_instance.operator,
            # just metadata
            task_metadata=self._get_metadata(task_instance, airflow_operator, dag_run),
        )

    # this method shouldn't be a public method of this class. Only ExternalTaskSensor
    # will have reason to use this method - we may as well move this method within 
    # get_metadata
    @staticmethod
    def external_task_sensor_run_ids(task_instance, airflow_operator, logical_date):
        def resolve_run_ids(airflow_operator, dates):
            from airflow.models import DagRun
            # why is DR needed - can't we directly use DagRun?
            DR = DagRun
            vals = []
            for d in dates:
                run_id = DR.find(
                    dag_id=airflow_operator.external_dag_id, execution_date=d
                )[0].run_id
                vals.append(run_id)
            return vals

        # can you add a url reference to the code instead on the comment below?
        # ----THIS CODE BLOCK USES LOGIC GIVEN IN EXTERNAL TASK SENSOR-----
        if airflow_operator.execution_delta:
            dttm = logical_date - airflow_operator.execution_delta
        elif airflow_operator.execution_date_fn:
            # `task_instance.get_template_context` : https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/taskinstance/index.html#airflow.models.taskinstance.TaskInstance.get_template_context
            dttm = airflow_operator._handle_execution_date_fn(
                context=task_instance.get_template_context()
            )
        else:
            dttm = logical_date
        dttm_filter = dttm if isinstance(dttm, list) else [dttm]
        # ------------------------------------------------------------
        resolvd_run_ids = resolve_run_ids(airflow_operator, dttm_filter)
        return resolvd_run_ids

    # I am not sure we need a function for this. you can simply generate a dictionary
    # in _get_metadata
    def _make_metadata(self, name, value):
        return dict(name=name, value=value)

    # rename this to metadata
    def _get_metadata(self, task_instance, airflow_operator, dag_run):
        metadata = []
        if task_instance.operator == "ExternalTaskSensor":
            metadata.extend(
                [
                    self._make_metadata(
                        "upstream-triggering-run-ids",
                        ", ".join(
                            self.external_task_sensor_run_ids(
                                task_instance, airflow_operator, dag_run.logical_date
                            )
                        ),
                    ),
                    self._make_metadata(
                        "upstream-dag-id", airflow_operator.external_dag_id
                    ),
                ]
            )
            if airflow_operator.external_task_id is not None:
                metadata.append(
                    self._make_metadata(
                        "upstream-task-id", str(airflow_operator.external_task_ids)
                    ),
                )
        elif task_instance.operator == "S3KeySensor":
            airflow_operator._resolve_bucket_and_key()
            metadata.extend(
                [
                    self._make_metadata("s3-key", airflow_operator.bucket_key),
                    self._make_metadata("s3-bucket-name", airflow_operator.bucket_name),
                ]
            )
        elif task_instance.operator == "SqlSensor":
            # todo : Should we store sql statement in the metadata ?
            # Storing SQL can raise security concerns and it would also require care when JSON.
            metadata.append(
                self._make_metadata("sql-conn-id", airflow_operator.conn_id)
            )
        return metadata


    def get_data(self):
        return self._data


PARENT_TASK_INSTANCE_STATUS_MACRO = (
    "{{ [task.upstream_task_ids, dag_run] | get_sensor_metadata }}"
)


def get_sensor_metadata(args):
    """
    This function will be a user defined macro that retrieve the task-instances for a task-id
    and figure its status so that we can pass it down to the airflow decorators and store it as metadata.

    It is used via the `PARENT_TASK_INSTANCE_STATUS_MACRO` to store this JSON dumped information in a environment variable.
    """

    task_ids, dag_run = args
    data = []
    dag = dag_run.get_dag()
    for tid in task_ids:
        # `task_instance` is of the form https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/taskinstance/index.html#airflow.models.taskinstance.TaskInstance
        task_instance = dag_run.get_task_instance(tid)
        operator_object = dag.get_task(tid)
        data.append(
            SensorMetaExtractor(task_instance, operator_object, dag_run).get_data()
        )
    return json.dumps(data)


###========================================================================
class AirflowDAGArgs(object):
    # _arg_types This object helps map types of
    # different keys that need to be parsed. None of the "values" in this
    # dictionary are being used. But the "types" of the values of are used when
    # reparsing the arguments from the config variable.

    # TODO: These values are being overriden in airflow.py. Can we list the
    #       sensible defaults directly here so that it is easier to grok the code.
    _arg_types = {
        "dag_id": "asdf",
        "description": "asdfasf",
        "schedule_interval": "*/2 * * * *",
        "start_date": datetime.now(),
        "catchup": False,
        "tags": [],
        "max_retry_delay": "",
        "dagrun_timeout": timedelta(minutes=60 * 4),
        "default_args": {
            "owner": "some_username",
            "depends_on_past": False,
            "email": ["some_email"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(seconds=10),
            "queue": "bash_queue",  #  which queue to target when running this job. Not all executors implement queue management, the CeleryExecutor does support targeting specific queues.
            "pool": "backfill",  # the slot pool this task should run in, slot pools are a way to limit concurrency for certain tasks
            "priority_weight": 10,
            "wait_for_downstream": False,
            "sla": timedelta(hours=2),
            "execution_timeout": timedelta(minutes=10),
            "trigger_rule": "all_success",
        },
    }

    metaflow_specific_args = {
        # Reference for user_defined_filters : https://stackoverflow.com/a/70175317
        "user_defined_filters": dict(
            hash=lambda my_value: hasher(my_value),
            task_id_creator=lambda v: task_id_creator(v),
            json_dump=lambda val: json_dump(val),
            get_sensor_metadata=lambda val: get_sensor_metadata(val),
            dash_connect=lambda val: dash_connect(val),
        ),
    }

    def __init__(self, **kwargs):
        self._args = kwargs

    @property
    def arguements(self): # TODO: Fix spelling
        return dict(**self._args, **self.metaflow_specific_args)

    # just serialize?
    def _serialize_args(self):
        def parse_args(dd):
            data_dict = {}
            for k, v in dd.items():
                # see the comment below for `from_dict`
                if k == "default_args":
                    data_dict[k] = parse_args(v)
                elif isinstance(v, datetime):
                    data_dict[k] = v.isoformat()
                elif isinstance(v, timedelta):
                    data_dict[k] = dict(seconds=v.total_seconds())
                else:
                    data_dict[k] = v
            return data_dict

        return parse_args(self._args)

    # just deserialize?
    @classmethod
    def from_dict(cls, data_dict):
        def parse_args(dd, type_check_dict):
            kwrgs = {}
            for k, v in dd.items():
                if k not in type_check_dict:
                    kwrgs[k] = v
                    continue
                # wouldn't you want to do this parsing for any type of nested structure
                # that is not datetime or timedelta? that should remove the reliance on
                # the magic word - default_args
                if k == "default_args":
                    kwrgs[k] = parse_args(v, type_check_dict[k])
                elif isinstance(type_check_dict[k], datetime):
                    kwrgs[k] = datetime.fromisoformat(v)
                elif isinstance(type_check_dict[k], timedelta):
                    kwrgs[k] = timedelta(**v)
                else:
                    kwrgs[k] = v
            return kwrgs

        return cls(**parse_args(data_dict, cls._arg_types))

    def to_dict(self):
        # dd is quite cryptic. why not just return self._serialize? also do we even need
        # this method? how about we just use `serialize`?
        dd = self._serialize_args()
        return dd

# TODO: This shouldn't be strictly needed?
def generate_rfc1123_name(flow_name, step_name):
    """
    Generate RFC 1123 compatible name. Specifically, the format is:
        <let-or-digit>[*[<let-or-digit-or-hyphen>]<let-or-digit>]

    The generated name consists from a human-readable prefix, derived from
    flow/step/task/attempt, and a hash suffux.
    """
    unique_str = "%s-%d" % (str(time.time()), random.randint(0, 1000))

    long_name = "-".join([flow_name, step_name, unique_str])
    hash = hashlib.sha256(long_name.encode("utf-8")).hexdigest()

    if long_name.startswith("_"):
        # RFC 1123 names can't start with hyphen so slap an extra prefix on it
        sanitized_long_name = "u" + long_name.replace("_", "-").lower()
    else:
        sanitized_long_name = long_name.replace("_", "-").lower()

    # the name has to be under 63 chars total
    return sanitized_long_name[:57] + "-" + hash[:5]

# better name - _kubernetes_pod_operator_args
# also, the output of this method is something that we should be able to generate
# statically on the user's workstation and not on Airflow server. basically - we can
# massage operator_args in the correct format before writing them out to the DAG file.
# that has a great side-effect of allowing us to eye-ball the results in the DAG file
# and not rely on more on-the-fly transformations.
def set_k8s_operator_args(flow_name, step_name, operator_args):
    from kubernetes import client
    from airflow.kubernetes.secret import Secret

    task_id = AIRFLOW_TASK_ID_TEMPLATE_VALUE
    # TODO: Reference RUN_ID_PREFIX here. Also this seems like the only place where
    #       dash_connect and hash are being used. Can we combine them together?
    run_id = "airflow-{{ [run_id, dag_run.dag_id] | dash_connect | hash }}"  # hash is added via the `user_defined_filters`
    attempt = "{{ task_instance.try_number - 1 }}"
    # Set dynamic env variables like run-id, task-id etc from here.
    env_vars = (
        [
            client.V1EnvVar(name=v["name"], value=str(v["value"]))
            for v in operator_args.get("env_vars", [])
        ]
        + [
            client.V1EnvVar(name=k, value=str(v))
            for k, v in dict(
                METAFLOW_RUN_ID=run_id,
                METAFLOW_AIRFLOW_TASK_ID=task_id,
                METAFLOW_AIRFLOW_DAG_RUN_ID="{{run_id}}",
                METAFLOW_AIRFLOW_JOB_ID="{{ti.job_id}}",
                METAFLOW_ATTEMPT_NUMBER=attempt,
            ).items()
        ]
        + [
            client.V1EnvVar(
                name=k,
                value_from=client.V1EnvVarSource(
                    field_ref=client.V1ObjectFieldSelector(field_path=str(v))
                ),
            )
            for k, v in {
                "METAFLOW_KUBERNETES_POD_NAMESPACE": "metadata.namespace",
                "METAFLOW_KUBERNETES_POD_NAME": "metadata.name",
                "METAFLOW_KUBERNETES_POD_ID": "metadata.uid",
            }.items()
        ]
    )

    labels = {
        "metaflow/attempt": attempt,
        "metaflow/run_id": run_id,
        "metaflow/task_id": task_id,
    }
    # We don't support volumes at the moment for `@kubernetes`
    volume_mounts = [
        client.V1VolumeMount(**v) for v in operator_args.get("volume_mounts", [])
    ]
    volumes = [client.V1Volume(**v) for v in operator_args.get("volumes", [])]
    secrets = [
        Secret("env", secret, secret) for secret in operator_args.get("secrets", [])
    ]
    args = {
        # "on_retry_callback": retry_callback,
        # use the default namespace even no namespace is defined rather than airflow
        "namespace": operator_args.get("namespace", "airflow"),
        # image is always available - no need for a fallback
        "image": operator_args.get("image", "python"),
        # we should be able to have a cleaner name - take a look at the argo implementation
        "name": generate_rfc1123_name(flow_name, step_name),
        "task_id": step_name,
        # do we need to specify args that are None? can we just rely on the system
        # defaults for such args?
        "random_name_suffix": None,
        "cmds": operator_args.get("cmds", []),
        "arguments": operator_args.get("arguments", []),
        # do we use ports?
        "ports": operator_args.get("ports", []),
        # do we use volume mounts?
        "volume_mounts": volume_mounts,
        # do we use volumes?
        "volumes": volumes,
        "env_vars": env_vars,
        # how are the values for env_from computed?
        "env_from": operator_args.get("env_from", []),
        "secrets": secrets,
        # will this ever be false?
        "in_cluster": operator_args.get(
            "in_cluster", True  #  run kubernetes client with in_cluster configuration.
        ),
        "labels": operator_args.get("labels", {}),
        "reattach_on_restart": False,
        # is there a default value we can rely on? we would ideally like the sys-admin
        # to be able to set these values inside their airflow deployment
        "startup_timeout_seconds": 120,
        "get_logs": True,  # This needs to be set to True to ensure that doesn't error out looking for xcom
        # we should use the default image pull policy
        "image_pull_policy": None,
        # annotations are not empty. see @kubernetes or argo-workflows
        "annotations": {},
        "resources": client.V1ResourceRequirements(
            # need to support disk and gpus - also the defaults don't match up to
            # the expected values. let's avoid adding defaults ourselves where we can.
            requests={
                "cpu": operator_args.get("cpu", 1),
                "memory": operator_args.get("memory", "2000M"),
            }
        ),  # kubernetes.client.models.v1_resource_requirements.V1ResourceRequirements
        "retries": operator_args.get("retries", 0),  # Base operator command
        
        "retry_exponential_backoff": False,  # todo : should this be a arg we allow on CLI. not right now - there is an open ticket for this - maybe at some point we will.
        "affinity": None,  # kubernetes.client.models.v1_affinity.V1Affinity
        "config_file": None,
        # image_pull_secrets : typing.Union[typing.List[kubernetes.client.models.v1_local_object_reference.V1LocalObjectReference], NoneType],
        "image_pull_secrets": operator_args.get("image_pull_secrets", []),
        "service_account_name": operator_args.get(  # Service account names can be essential for passing reference to IAM roles etc.
            "service_account_name", None
        ),
        # let's rely on the default values
        "is_delete_operator_pod": operator_args.get(
            "is_delete_operator_pod", False
        ),  # if set to true will delete the pod once finished /failed execution. By default it is true
        # let's rely on the default values
        "hostnetwork": False,  # If True enable host networking on the pod.
        "security_context": {},
        "log_events_on_failure": True,
        "do_xcom_push": True,
    }
    args["labels"].update(labels)
    if operator_args.get("execution_timeout", None):
        args["execution_timeout"] = timedelta(
            **operator_args.get(
                "execution_timeout",
            )
        )
    if operator_args.get("retry_delay", None):
        args["retry_delay"] = timedelta(**operator_args.get("retry_delay"))
    return args

# why do we need a separate method for this function? can we just embed the logic in
# _kubernetes_task?
def get_k8s_operator():

    try:
        from airflow.contrib.operators.kubernetes_pod_operator import (
            KubernetesPodOperator,
        )
    except ImportError:
        try:
            from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
                KubernetesPodOperator,
            )
        except ImportError as e:
            raise KubernetesProviderNotFound(
                "This DAG requires a `KubernetesPodOperator`. "
                "Install the Airflow Kubernetes provider using : "
                "`pip install apache-airflow-providers-cncf-kubernetes`"
            )

    return KubernetesPodOperator


def _parse_sensor_args(name, kwargs):
    if name == SensorNames.EXTERNAL_TASK_SENSOR:
        if "execution_delta" in kwargs:
            if type(kwargs["execution_delta"]) == dict:
                kwargs["execution_delta"] = timedelta(**kwargs["execution_delta"])
            else:
                del kwargs["execution_delta"]
    return kwargs


def _get_sensor(name):
    if name == SensorNames.EXTERNAL_TASK_SENSOR:
        # ExternalTaskSensors uses an execution_date of a dag to
        # determine the appropriate DAG.
        # This is set to the exact date the current dag gets executed on.
        # For example if "DagA" (Upstream DAG) got scheduled at
        # 12 Jan 4:00 PM PDT then "DagB"(current DAG)'s task sensor will try to
        # look for a "DagA" that got executed at 12 Jan 4:00 PM PDT **exactly**.
        # They also support a `execution_timeout` argument to
        from airflow.sensors.external_task_sensor import ExternalTaskSensor

        return ExternalTaskSensor
    elif name == SensorNames.S3_SENSOR:
        try:
            from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
        except ImportError:
            raise AirflowSensorNotFound(
                "This DAG requires a `S3KeySensor`. "
                "Install the Airflow AWS provider using : "
                "`pip install apache-airflow-providers-amazon`"
            )
        return S3KeySensor
    elif name == SensorNames.SQL_SENSOR:
        from airflow.sensors.sql import SqlSensor

        return SqlSensor


class AirflowTask(object):
    def __init__(self, name, operator_type="kubernetes", flow_name=None):
        self.name = name
        self._operator_args = None
        self._operator_type = operator_type
        self._flow_name = flow_name

    def set_operator_args(self, **kwargs):
        self._operator_args = kwargs
        return self

    def _make_sensor(self):
        TaskSensor = _get_sensor(self._operator_type)
        return TaskSensor(
            task_id=self.name,
            **_parse_sensor_args(self._operator_type, self._operator_args)
        )

    def to_dict(self):
        return {
            "name": self.name,
            "operator_type": self._operator_type,
            "operator_args": self._operator_args,
        }

    @classmethod
    def from_dict(cls, jsd, flow_name=None):
        op_args = {} if not "operator_args" in jsd else jsd["operator_args"]
        return cls(
            jsd["name"],
            operator_type=jsd["operator_type"]
            if "operator_type" in jsd
            else "kubernetes",
            flow_name=flow_name,
        ).set_operator_args(**op_args)

    def _kubenetes_task(self):
        KubernetesPodOperator = get_k8s_operator()
        k8s_args = set_k8s_operator_args(
            self._flow_name, self.name, self._operator_args
        )
        return KubernetesPodOperator(**k8s_args)

    def to_task(self):
        if self._operator_type == "kubernetes":
            return self._kubenetes_task()
        elif self._operator_type in SensorNames.get_supported_sensors():
            return self._make_sensor()


class Workflow(object):
    def __init__(self, file_path=None, graph_structure=None, **kwargs):
        self._dag_instantiation_params = AirflowDAGArgs(**kwargs)
        self._file_path = file_path
        tree = lambda: defaultdict(tree)
        self.states = tree()
        self.metaflow_params = None
        self.graph_structure = graph_structure

    def set_parameters(self, params):
        self.metaflow_params = params

    def add_state(self, state):
        self.states[state.name] = state

    def to_dict(self):
        return dict(
            graph_structure=self.graph_structure,
            states={s: v.to_dict() for s, v in self.states.items()},
            dag_instantiation_params=self._dag_instantiation_params.to_dict(),
            file_path=self._file_path,
            metaflow_params=self.metaflow_params,
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data_dict):
        re_cls = cls(
            file_path=data_dict["file_path"],
            graph_structure=data_dict["graph_structure"],
        )
        re_cls._dag_instantiation_params = AirflowDAGArgs.from_dict(
            data_dict["dag_instantiation_params"]
        )

        for sd in data_dict["states"].values():
            re_cls.add_state(
                AirflowTask.from_dict(
                    sd, flow_name=re_cls._dag_instantiation_params.arguements["dag_id"]
                )
            )
        re_cls.set_parameters(data_dict["metaflow_params"])
        return re_cls

    @classmethod
    def from_json(cls, json_string):
        data = json.loads(json_string)
        return cls.from_dict(data)

    def _construct_params(self):
        from airflow.models.param import Param

        if self.metaflow_params is None:
            return {}
        param_dict = {}
        for p in self.metaflow_params:
            name = p["name"]
            del p["name"]
            param_dict[name] = Param(**p)
        return param_dict

    def compile(self):
        from airflow import DAG

        params_dict = self._construct_params()
        # DAG Params can be seen here :
        # https://airflow.apache.org/docs/apache-airflow/2.0.0/_api/airflow/models/dag/index.html#airflow.models.dag.DAG
        # Airflow 2.0.0 Allows setting Params.
        dag = DAG(params=params_dict, **self._dag_instantiation_params.arguements)
        dag.fileloc = self._file_path if self._file_path is not None else dag.fileloc

        def add_node(node, parents, dag):
            """
            A recursive function to traverse the specialized
            graph_structure datastructure.
            """
            if type(node) == str:
                task = self.states[node].to_task()
                if parents:
                    for parent in parents:
                        parent >> task
                return [task]  # Return Parent

            # this means a split from parent
            if type(node) == list:
                # this means branching since everything within the list is a list
                if all(isinstance(n, list) for n in node):
                    curr_parents = parents
                    parent_list = []
                    for node_list in node:
                        last_parent = add_node(node_list, curr_parents, dag)
                        parent_list.extend(last_parent)
                    return parent_list
                else:
                    # this means no branching and everything within the list is not a list and can be actual nodes.
                    curr_parents = parents
                    for node_x in node:
                        curr_parents = add_node(node_x, curr_parents, dag)
                    return curr_parents

        with dag:
            parent = None
            for node in self.graph_structure:
                parent = add_node(node, parent, dag)

        return dag
