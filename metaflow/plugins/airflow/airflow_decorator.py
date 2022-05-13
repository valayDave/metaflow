from datetime import timedelta
import os
import json
import time

from metaflow.decorators import FlowDecorator, StepDecorator

from metaflow.metadata import MetaDatum

from .airflow_utils import TASK_ID_XCOM_KEY, AirflowTask, SensorNames


K8S_XCOM_DIR_PATH = "/airflow/xcom"


def safe_mkdir(dir):
    try:
        os.makedirs(dir)
    except FileExistsError:
        pass


def push_xcom_values(xcom_dict):
    safe_mkdir(K8S_XCOM_DIR_PATH)
    with open(os.path.join(K8S_XCOM_DIR_PATH, "return.json"), "w") as f:
        json.dump(xcom_dict, f)


AIRFLOW_STATES = dict(
    QUEUED="queued",
    RUNNING="running",
    SUCCESS="success",
    SHUTDOWN="shutdown",  # External request to shut down,
    FAILED="failed",
    UP_FOR_RETRY="up_for_retry",
    UP_FOR_RESCHEDULE="up_for_reschedule",
    UPSTREAM_FAILED="upstream_failed",
    SKIPPED="skipped",
)


def _get_sensor_exception():
    from .airflow_compiler import AirflowException

    class AirflowSensorException(AirflowException):
        pass

    return AirflowSensorException


def _arg_exception(arg_name, deconame, value, allowed_values=None, allowed_type=None):
    msg_str = "`%s` cannot be `%s` when using @%s" % (
        arg_name,
        str(value),
        deconame,
    )
    if allowed_type is not None:
        msg_str = "`%s` cannot be `%s` when using @%s. Accepted type of `%s` is %s" % (
            arg_name,
            str(value),
            deconame,
            arg_name,
            allowed_type,
        )
    elif allowed_values is not None:
        msg_str = "`%s` cannot be `%s` when using @%s. Accepted values are : %s" % (
            arg_name,
            str(value),
            deconame,
            ", ".join(allowed_values),
        )
    return _get_sensor_exception()(msg_str)


class AirflowScheduleIntervalDecorator(FlowDecorator):
    name = "airflow_schedule_interval"
    defaults = {"cron": None, "weekly": False, "daily": True, "hourly": False}

    options = {
        "schedule": dict(
            default=None,
            show_default=False,
            help="Cron schedule for the Airflow DAG. "
            "Accepts cron schedules and airflow presets like"
            "@daily, @hourly, @weekly,",
        )
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        self._option_values = options

        if self._option_values["schedule"]:
            self.schedule = self._option_values["schedule"]
        elif self.attributes["cron"]:
            self.schedule = self.attributes["cron"]
        elif self.attributes["weekly"]:
            self.schedule = "@weekly"
        elif self.attributes["hourly"]:
            self.schedule = "@hourly"
        elif self.attributes["daily"]:
            self.schedule = "@daily"
        else:
            self.schedule = None

    def get_top_level_options(self):
        return list(dict(schedule=self.schedule).items())


class AirflowSensorDecorator(FlowDecorator):
    """
    Base class for all Airflow sensor decorators.
    """

    allow_multiple = True

    defaults = dict(
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
        exponential_backoff=True,
        pool=None,
        soft_fail=False,
        name=None,
        description=None,
    )

    operator_type = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._task_name = self.operator_type

    def serialize_operator_args(self):
        """
        Subclasses will parse the decorator arguments to
        Airflow task serializable arguments.
        """
        task_args = dict(**self.attributes)
        del task_args["name"]
        if task_args["description"] is not None:
            task_args["doc"] = task_args["description"]
        del task_args["description"]
        task_args["do_xcom_push"] = True
        return task_args

    def create_task(self):
        task_args = self.serialize_operator_args()
        return AirflowTask(
            self._task_name,
            operator_type=self.operator_type,
        ).set_operator_args(**{k: v for k, v in task_args.items() if v is not None})

    def compile(self):
        """
        compile the arguments for `airflow create` command.
        This will even check if the arguments are acceptible.
        """
        # If there are more than one sensor decorators then ensure that `name` is set
        # so that we can have uniqueness of `task_id` when creating tasks on airflow.
        sensor_decorators = [
            d
            for d in self._flow_decorators
            if issubclass(d.__class__, AirflowSensorDecorator)
        ]
        sensor_deco_types = {}
        for d in sensor_decorators:
            if d.__class__.__name__ not in sensor_deco_types:
                sensor_deco_types[d.__class__.__name__] = []
            sensor_deco_types[d.__class__.__name__].append(d)
        # If there are more than one decorator per sensor-type then we require the name argument.
        if sum([len(v) for v in sensor_deco_types.values()]) > len(sensor_deco_types):
            if self.attributes["name"] is None:
                raise _get_sensor_exception()(
                    "`name` argument cannot be `None` when multiple Airflow Sensor related decorators are attached to a flow."
                )
        if self.attributes["name"] is not None:
            self._task_name = self.attributes["name"]

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        self.compile()


class ExternalTaskSensorDecorator(AirflowSensorDecorator):
    operator_type = SensorNames.EXTERNAL_TASK_SENSOR
    # Docs:
    # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/external_task/index.html#airflow.sensors.external_task.ExternalTaskSensor
    name = "airflow_external_task_sensor"
    defaults = dict(
        **AirflowSensorDecorator.defaults,
        external_dag_id=None,
        external_task_ids=None,
        allowed_states=["success"],
        failed_states=None,
        execution_delta=None,
        check_existence=True,
        # we cannot add `execution_date_fn` as it requires a python callable.
        # Passing around a python callable is non-trivial since we are passing a
        # callable from metaflow-code to airflow python script. In this conversion we cannot
        # that the callable will as the user expects since we cannot transfer dependencies
    )

    def serialize_operator_args(self):
        task_args = super().serialize_operator_args()
        if task_args["execution_delta"] is not None:
            task_args["execution_delta"] = dict(
                seconds=task_args["execution_delta"].total_seconds()
            )
        return task_args

    def compile(self):
        if self.attributes["external_dag_id"] is None:
            raise _arg_exception("external_dag_id", self.name, None)

        if type(self.attributes["allowed_states"]) == str:
            if self.attributes["allowed_states"] not in list(AIRFLOW_STATES.values()):
                raise _arg_exception(
                    "allowed_states",
                    self.name,
                    self.attributes["allowed_states"],
                    list(AIRFLOW_STATES.values()),
                )
        elif type(self.attributes["allowed_states"]) == list:
            fst = [
                x
                for x in self.attributes["allowed_states"]
                if x not in list(AIRFLOW_STATES.values())
            ]
            if len(fst) > 0:
                raise _arg_exception(
                    "allowed_states",
                    self.name,
                    ", ".join(fst),
                    list(AIRFLOW_STATES.values()),
                )
        else:
            self.attributes["allowed_states"] = ["success"]

        if self.attributes["execution_delta"] is not None:
            if not isinstance(self.attributes["execution_delta"], timedelta):
                raise _arg_exception(
                    "execution_delta",
                    self.name,
                    self.attributes["execution_delta"],
                    allowed_type="datetime.timedelta",
                )
        super().compile()


class S3KeySensorDecorator(AirflowSensorDecorator):
    name = "airflow_s3_key_sensor"
    operator_type = SensorNames.S3_SENSOR
    # Arg specification can be found here :
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/s3/index.html#airflow.providers.amazon.aws.sensors.s3.S3KeySensor
    defaults = dict(
        **AirflowSensorDecorator.defaults,
        bucket_key=None,  # Required
        bucket_name=None,
        wildcard_match=False,
        aws_conn_id=None,
        verify=None,  # `verify (Optional[Union[str, bool]])` Whether or not to verify SSL certificates for S3 connection.
        #  `verify` is a airflow variable.
    )

    def compile(self):
        if self.attributes["bucket_key"] is None:
            raise _arg_exception("bucket_key", self.name, None)
        super().compile()


class SQLSensorDecorator(AirflowSensorDecorator):
    name = "airflow_sql_sensor"
    operator_type = SensorNames.SQL_SENSOR
    # Arg specification can be found here :
    # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/sql/index.html#airflow.sensors.sql.SqlSensor
    defaults = dict(
        **AirflowSensorDecorator.defaults,
        conn_id=None,
        sql=None,
        # success = None, # sucess/failure require callables. Wont be supported at start since not serialization friendly.
        # failure = None,
        parameters=None,
        fail_on_empty=True,
    )

    def compile(self):
        if self.attributes["conn_id"] is None:
            raise _arg_exception("conn_id", self.name, None)
        if self.attributes["sql"] is None:
            raise _arg_exception("sql", self.name, None)
        super().compile()


class AirflowInternalDecorator(StepDecorator):
    name = "airflow_internal"

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        # find out where the execution is taking place.
        # Once figured where the execution is happening then we can do
        # handle xcom push / pull differently
        meta = {}
        meta["airflow-dag-run-id"] = os.environ["METAFLOW_AIRFLOW_DAG_RUN_ID"]
        meta["airflow-job-id"] = os.environ["METAFLOW_AIRFLOW_JOB_ID"]
        entries = [
            MetaDatum(
                field=k, value=v, type=k, tags=["attempt_id:{0}".format(retry_count)]
            )
            for k, v in meta.items()
        ]

        # Register book-keeping metadata for debugging.
        metadata.register_metadata(run_id, step_name, task_id, entries)
        push_xcom_values(
            {
                TASK_ID_XCOM_KEY: os.environ["METAFLOW_AIRFLOW_TASK_ID"],
            }
        )


SUPPORTED_SENSORS = [
    ExternalTaskSensorDecorator.name,
    S3KeySensorDecorator.name,
    SQLSensorDecorator.name,
]
