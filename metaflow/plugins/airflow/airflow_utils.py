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
RUN_ID_PREFIX = "airflow"

# AIRFLOW_TASK_ID_TEMPLATE_VALUE will work for linear/branched workflows.
# ti.task_id is the stepname in metaflow code.
# AIRFLOW_TASK_ID_TEMPLATE_VALUE uses a jinja filter called `task_id_creator` which helps
# concatenate the string using a `/`. Since run-id will keep changing and stepname will be
# the same task id will change. Since airflow doesn't encourage dynamic rewriting of dags
# we can rename steps in a foreach with indexes (eg. `stepname-$index`) to create those steps.
# Hence : Foreachs will require some special form of plumbing.
# https://stackoverflow.com/questions/62962386/can-an-airflow-task-dynamically-generate-a-dag-at-runtime
AIRFLOW_TASK_ID_TEMPLATE_VALUE = (
    "%s-{{ [run_id, ti.task_id, dag_run.dag_id ] | task_id_creator  }}" % RUN_ID_PREFIX
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


def run_id_creator(val):
    # join `[dag-id,run-id]` of airflow dag. 
    return hashlib.md5("-".join(val).encode("utf-8")).hexdigest()[:RUN_ID_LEN]
    

def task_id_creator(lst):
    # This is a filter which creates a hash of the run_id/step_name string.
    # Since run_ids in airflow are constants, they don't create an issue with the
    #
    return hashlib.md5("/".join(lst).encode("utf-8")).hexdigest()[:TASK_ID_LEN]


def json_dump(val):
    return json.dumps(val)


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
            task_id_creator=lambda v: task_id_creator(v),
            json_dump=lambda val: json_dump(val),
            run_id_creator=lambda val:run_id_creator(val)
        ),
    }

    def __init__(self, **kwargs):
        self._args = kwargs

    @property
    def arguments(self):
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

def _kubernetes_pod_operator_args(flow_name, step_name, operator_args):
    from kubernetes import client
    
    from airflow.kubernetes.secret import Secret
    # Set dynamic env variables like run-id, task-id etc from here.
    secrets = [
        Secret("env", secret, secret) for secret in operator_args.get("secrets", [])
    ]
    args = operator_args
    args.update({
        # todo : we should be able to have a cleaner name - take a look at the argo implementation
        "name": generate_rfc1123_name(flow_name, step_name),
        "secrets": secrets,        
        # Question for (savin): 
            # Default timeout in airflow is 120. I can remove `startup_timeout_seconds` for now. how should we expose it to the user? 
        
        # todo :annotations are not empty. see @kubernetes or argo-workflows
        "annotations": {},

    })
    # Below cannot be passed in dictionary form. After trying a few times it didin't work.
    additional_env_vars = [
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
    args["env_vars"] = [client.V1EnvVar(name=x["name"],value=x["value"]) for x in args["env_vars"]] + additional_env_vars
    
    # We need to explicitly parse resources to k8s.V1ResourceRequirements otherwise airflow tries 
    # to parse dictionaries to `airflow.providers.cncf.kubernetes.backcompat.pod.Resources` object via 
    # `airflow.providers.cncf.kubernetes.backcompat.backward_compat_converts.convert_resources` function
    resources = args.get("resources")
    args["resources"] = client.V1ResourceRequirements(
        requests = resources['requests'],
    )
    if operator_args.get("execution_timeout", None):
        args["execution_timeout"] = timedelta(
            **operator_args.get(
                "execution_timeout",
            )
        )
    if operator_args.get("retry_delay", None):
        args["retry_delay"] = timedelta(**operator_args.get("retry_delay"))
    return args


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
        k8s_args = _kubernetes_pod_operator_args(
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
                    sd, flow_name=re_cls._dag_instantiation_params.arguments["dag_id"]
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
        dag = DAG(params=params_dict, **self._dag_instantiation_params.arguments)
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
