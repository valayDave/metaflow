import json
import os
import time
from datetime import timedelta

from metaflow.decorators import FlowDecorator, StepDecorator
from metaflow.metadata import MetaDatum
from .exception import AirflowException

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
        schedule_interval = flow._flow_decorators.get("airflow_schedule_interval")
        schedule = flow._flow_decorators.get("schedule")
        if schedule is not None and schedule_interval is not None:
            raise AirflowException(
                "Flow cannot have @schedule and @airflow_schedule_interval at the same time. Use any one."
            )

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
        # todo (savin-comments): fix this comment.
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
