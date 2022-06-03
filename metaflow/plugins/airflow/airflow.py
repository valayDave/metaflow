import base64
import json
import os
import random
import string
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import metaflow.util as util
from metaflow.decorators import flow_decorators
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (BATCH_METADATA_SERVICE_HEADERS,
                                      BATCH_METADATA_SERVICE_URL,
                                      DATASTORE_CARD_S3ROOT,
                                      DATASTORE_SYSROOT_S3, DATATOOLS_S3ROOT,
                                      KUBERNETES_SERVICE_ACCOUNT)
from metaflow.parameters import deploy_time_eval
from metaflow.plugins.kubernetes.kubernetes import Kubernetes
from metaflow.plugins.cards.card_modules import chevron
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.util import dict_to_cli_options, get_username, compress_list

from . import airflow_utils
from .airflow_decorator import SUPPORTED_SENSORS, AirflowSensorDecorator
from .airflow_utils import (AIRFLOW_TASK_ID_TEMPLATE_VALUE,
                            RUN_ID_LEN,
                            TASK_ID_XCOM_KEY, AirflowTask, Workflow)

AIRFLOW_DEPLOY_TEMPLATE_FILE = os.path.join(os.path.dirname(__file__), "dag.py")


RUN_ID_PREFIX = "airflow"

class AirflowException(MetaflowException):
    headline = "Airflow Exception"

    def __init__(self, msg):
        super().__init__(msg)


class NotSupportedException(MetaflowException):
    headline = "Not yet supported with Airflow"


class Airflow(object):

    parameter_macro = "{{ params | json_dump }}"

    task_id = AIRFLOW_TASK_ID_TEMPLATE_VALUE
    task_id_arg = "--task-id %s" % task_id

    # Airflow run_ids are of the form : "manual__2022-03-15T01:26:41.186781+00:00"
    # Such run-ids break the `metaflow.util.decompress_list`; this is why we hash the runid
    run_id = (
        "%s-$(echo -n {{ run_id }}-{{ dag_run.dag_id }} | md5sum | awk '{print $1}' | awk '{print substr ($0, 0, %s)}')"
        % (RUN_ID_PREFIX, str(RUN_ID_LEN))
    )
    # We do echo -n because emits line breaks and we dont want to consider that since it we want same hash value when retrieved in python.
    run_id_arg = "--run-id %s" % run_id
    attempt = "{{ task_instance.try_number - 1 }}"

    def __init__(
        self,
        name,
        graph,
        flow,
        code_package_sha,
        code_package_url,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        tags=None,
        namespace=None,
        username=None,
        max_workers=None,
        worker_pool=None,
        description=None,
        file_path=None,
        is_paused_upon_creation=True,
    ):
        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package_sha = code_package_sha
        self.code_package_url = code_package_url
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags
        self.namespace = namespace  # this is the username space
        self.username = username
        self.max_workers = max_workers
        self.description = description
        self._depends_on_upstream_sensors = False
        self._file_path = file_path
        self.parameters = None
        _, self.graph_structure = self.graph.output_steps()
        self.worker_pool = worker_pool
        # replace with `is_paused_upon_creation`
        self.is_paused_upon_creation = is_paused_upon_creation
        self._set_scheduling_interval()

    def _set_scheduling_interval(self):
        """
        The airflow integration allows setting schedule interval using `@schedule` and `@airflow_schedule_interval` decorator. 
        This method will extract interval from both and apply the one which is not None. We raise an exception in the 
        airflow_cli.py if both flow decorators are set. 
        """
        schedule_decorator_sint, airflow_schedule_decorator_sint = self._get_schedule(), self._get_airflow_schedule_interval()
        self.schedule_interval = None
        if schedule_decorator_sint is not None:
            self.schedule_interval = schedule_decorator_sint
        elif airflow_schedule_decorator_sint is not None:
            self.schedule_interval = airflow_schedule_decorator_sint

    def _get_schedule(self):
        # Using the cron presets provided here :
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html?highlight=schedule%20interval#cron-presets
        schedule = self.flow._flow_decorators.get("schedule")
        if not schedule:
            return None
        if schedule.attributes["cron"]:
            return schedule.attributes["cron"]
        elif schedule.attributes["weekly"]:
            return "@weekly"
        elif schedule.attributes["hourly"]:
            return "@hourly"
        elif schedule.attributes["daily"]:
            return "@daily"
        return None

    def _get_airflow_schedule_interval(self):
        schedule_interval = self.flow._flow_decorators.get("airflow_schedule_interval")
        if schedule_interval is None:
            return None
        return schedule_interval.schedule    

    def _get_retries(self, node):
        max_user_code_retries = 0
        max_error_retries = 0
        # Different decorators may have different retrying strategies, so take
        # the max of them.
        for deco in node.decorators:
            user_code_retries, error_retries = deco.step_task_retry_count()
            max_user_code_retries = max(max_user_code_retries, user_code_retries)
            max_error_retries = max(max_error_retries, error_retries)

        return max_user_code_retries, max_user_code_retries + max_error_retries

    def _get_retry_delay(self, node):
        retry_decos = [deco for deco in node.decorators if deco.name == "retry"]
        if len(retry_decos) > 0:
            retry_mins = retry_decos[0].attributes["minutes_between_retries"]
            return timedelta(minutes=int(retry_mins))
        return None

    def _process_parameters(self):
        parameters = []
        seen = set()
        airflow_params = []
        type_transform_dict = {
            int.__name__: "integer",
            str.__name__: "string",
            bool.__name__: "string",
            float.__name__: "number",
        }
        type_parser = {bool.__name__: lambda v: str(v)}

        for var, param in self.flow._get_parameters():
            # Throw an exception if the parameter is specified twice.
            norm = param.name.lower()
            if norm in seen:
                raise MetaflowException(
                    "Parameter *%s* is specified twice. "
                    "Note that parameter names are "
                    "case-insensitive." % param.name
                )
            seen.add(norm)

            is_required = param.kwargs.get("required", False)
            # Throw an exception if a schedule is set for a flow with required
            # parameters with no defaults.
            if "default" not in param.kwargs and is_required:
                raise MetaflowException(
                    "The parameter *%s* does not have a "
                    "default while having 'required' set to 'True'. "
                    "A default is required for such parameters when deploying on Airflow."
                )
            if "default" not in param.kwargs and self.schedule_interval:
                raise MetaflowException(
                    "When @schedule is set with Airflow, Parameters require default values. "
                    "The parameter *%s* does not have a "
                    "'default' set"
                )
            value = deploy_time_eval(param.kwargs.get("default"))
            parameters.append(dict(name=param.name, value=value))
            
            # Setting airflow related param args.
            param_type = param.kwargs.get("type", None)
            airflow_param = dict(
                name=param.name,
            )
            param_help = param.kwargs.get("help", None)
            if value is not None:
                airflow_param["default"] = value
            if param_help:
                airflow_param["description"] = param_help
            
            if param_type is not None and param_type.__name__ in type_transform_dict:
                airflow_param["type"] = type_transform_dict[param_type.__name__]
                if param_type.__name__ in type_parser and value is not None:
                    airflow_param["default"] = type_parser[param_type.__name__](value)

            airflow_params.append(airflow_param)
        self.parameters = airflow_params

        return parameters

    def _make_input_path_compressed(
        self,
        step_names,
    ):
        return compress_list(
            [ self._make_input_path(s) for s in step_names ]
        )

    def _make_input_path(self, step_name):
        # This is set using the `airflow_internal` decorator.
        # This will pull the `return_value` xcom which holds a dictionary.
        # This helps pass state.
        task_id_string = "/%s/{{ task_instance.xcom_pull(task_ids='%s')['%s'] }}" % (
            step_name,
            step_name,
            TASK_ID_XCOM_KEY,
        )

        return "%s%s" % (self.run_id, task_id_string)

    def _to_job(self, node):
        """
        This function will transform the node's specification into Airflow compatible operator arguements. 
        Since this function is long. We it performs two major duties:
            1. Based on the type of the graph node (start/linear/foreach/join etc.) it will decide how to set the input paths
            2. Based on node's decorator specification convert the information into a job spec for the KubernetesPodOperator. 
        """
        # supported compute : k8s (v1), local(v2), batch(v3)
        # Add env vars from the optional @environment decorator.
        env_deco = [deco for deco in node.decorators if deco.name == "environment"]
        env = {}
        if env_deco:
            env = env_deco[0].attributes["vars"]

        # The Below If/Else Block handle "Input Paths".
        # Input Paths help manage dataflow across the graph.
        if node.name == "start":
            # POSSIBLE_FUTURE_IMPROVEMENT: 
                # We can extract metadata about the possible upstream sensor triggers. 
                # There is a previous commit (7bdf6) in the `airflow` branch that has `SensorMetaExtractor` class and 
                # associated MACRO we have built to handle this case if a metadata regarding the sensor is needed. 
            # Initialize parameters for the flow in the `start` step.
            # `start` step has no upstream input dependencies aside from
            # parameters.
            parameters = self._process_parameters()
            if parameters:
                env["METAFLOW_PARAMETERS"] = self.parameter_macro
                default_parameters = {}
                for parameter in parameters:
                    if parameter["value"] is not None:
                        default_parameters[parameter["name"]] = parameter["value"]
                # Dump the default values specified in the flow.
                env["METAFLOW_DEFAULT_PARAMETERS"] = json.dumps(default_parameters)
            input_paths = None
        else:
            # If it is not the start node then we check if there are many paths
            # converging into it or a single path. Based on that we set the INPUT_PATHS
            if node.parallel_foreach:
                raise AirflowException(
                    "Parallel steps are not supported yet with Airflow."
                )

            if len(node.in_funcs) == 1:
                # set input paths where this is only one parent node
                # The parent-task-id is passed via the xcom; There is no other way to get that.
                # One key thing about xcoms is that they are immutable and only accepted if the task
                # doesn't fail.
                # From airflow docs :
                # "Note: If the first task run is not succeeded then on every retry task XComs will be cleared to make the task run idempotent."
                input_paths = self._make_input_path(node.in_funcs[0])
            else:
                # this is a split scenario where there can be more than one input paths.
                input_paths = self._make_input_path_compressed(node.in_funcs)

            env["METAFLOW_INPUT_PATHS"] = input_paths

        env["METAFLOW_CODE_URL"] = self.code_package_url
        env["METAFLOW_FLOW_NAME"] = self.flow.name
        env["METAFLOW_STEP_NAME"] = node.name
        env["METAFLOW_OWNER"] = self.username

        metadata_env = self.metadata.get_runtime_environment("airflow")
        env.update(metadata_env)

        metaflow_version = self.environment.get_environment_info()
        metaflow_version["flow_name"] = self.graph.name
        env["METAFLOW_VERSION"] = json.dumps(metaflow_version)

        # Todo : Find ways to pass state using for the below usecases:
        #   1. To set the cardinality of foreaches
        #   2. To set the input paths from the parent steps of a foreach join.
        #   3. To read the input paths in a foreach join.

        # Extract the k8s decorators for constructing the arguments of the K8s Pod Operator on Airflow. 
        k8s_deco = [deco for deco in node.decorators if deco.name == "kubernetes"][0]
        user_code_retries, _ = self._get_retries(node)
        retry_delay = self._get_retry_delay(node)
        # This sets timeouts for @timeout decorators.
        # The timeout is set as "execution_timeout" for an airflow task.
        runtime_limit = get_run_time_limit_for_task(node.decorators)

        k8s = Kubernetes(
            self.flow_datastore, self.metadata, self.environment,
        )
        user = util.get_username()
        labels = {
            "app": "metaflow",
            "app.kubernetes.io/name": "metaflow-task",
            "app.kubernetes.io/part-of": "metaflow",
            "app.kubernetes.io/created-by": user,
            # Question to (savin) : Should we have username set over here for created by since it is the airflow installation that is creating the jobs. 
            # Technically the "user" is the stakeholder but should these labels be present. 
        }
        additional_mf_variables = {
            "METAFLOW_CODE_SHA": self.code_package_sha,
            "METAFLOW_CODE_URL": self.code_package_url,
            "METAFLOW_CODE_DS": self.datastore.TYPE,
            "METAFLOW_USER": user,
            "METAFLOW_SERVICE_URL": BATCH_METADATA_SERVICE_URL,
            "METAFLOW_SERVICE_HEADERS": json.dumps(BATCH_METADATA_SERVICE_HEADERS),
            "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
            "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
            "METAFLOW_DEFAULT_DATASTORE": "s3",
            "METAFLOW_DEFAULT_METADATA": "service",
            # Question for (savin) : what does `METAFLOW_KUBERNETES_WORKLOAD` do ? 
            "METAFLOW_KUBERNETES_WORKLOAD": 1,
            "METAFLOW_RUNTIME_ENVIRONMENT": "kubernetes",
            "METAFLOW_CARD_S3ROOT": DATASTORE_CARD_S3ROOT,
        }
        env.update(additional_mf_variables)
        
        service_account = k8s_deco.attributes["service_account"]

        k8s_operator_args = dict(
            namespace=k8s_deco.attributes["namespace"],
            service_account_name=KUBERNETES_SERVICE_ACCOUNT
            if service_account is None
            else service_account,
            node_selector=k8s_deco.attributes["node_selector"],
            cmds=k8s._command(
                self.flow.name, self.run_id, node.name, self.task_id, self.attempt,
                code_package_url=self.code_package_url,
                step_cmds= self._step_cli(node, input_paths, self.code_package_url, user_code_retries),
            ),
            in_cluster=True,
            image=k8s_deco.attributes["image"],
            cpu=k8s_deco.attributes["cpu"],
            memory=k8s_deco.attributes["memory"],
            disk=k8s_deco.attributes["disk"],
            execution_timeout=dict(seconds=runtime_limit),
            retry_delay=dict(seconds=retry_delay.total_seconds()) if retry_delay else None,
            retries=user_code_retries,
            env_vars=[dict(name=k, value=v) for k, v in env.items()],
            labels=labels,
            is_delete_operator_pod=True,
        )
        if k8s_deco.attributes["secrets"]:
            k8s_operator_args["secrets"] = k8s_deco.attributes["secrets"]

        return k8s_operator_args

    def _step_cli(self, node, paths, code_package_url, user_code_retries):
        cmds = []

        script_name = os.path.basename(sys.argv[0])
        executable = self.environment.executable(node.name)

        entrypoint = [executable, script_name]

        top_opts_dict = {
            "with": [
                decorator.make_decorator_spec()
                for decorator in node.decorators
                if not decorator.statically_defined
            ]
        }
        # FlowDecorators can define their own top-level options. They are
        # responsible for adding their own top-level options and values through
        # the get_top_level_options() hook. See similar logic in runtime.py.
        for deco in flow_decorators():
            top_opts_dict.update(deco.get_top_level_options())

        top_opts = list(dict_to_cli_options(top_opts_dict))

        top_level = top_opts + [
            "--quiet",
            "--metadata=%s" % self.metadata.TYPE,
            "--environment=%s" % self.environment.TYPE,
            "--datastore=%s" % self.flow_datastore.TYPE,
            "--datastore-root=%s" % self.flow_datastore.datastore_root,
            "--event-logger=%s" % self.event_logger.logger_type,
            "--monitor=%s" % self.monitor.monitor_type,
            "--no-pylint",
            "--with=airflow_internal",
        ]

        if node.name == "start":
            # We need a separate unique ID for the special _parameters task
            task_id_params = "%s-params" % self.task_id
            # Export user-defined parameters into runtime environment
            param_file = "".join(
                random.choice(string.ascii_lowercase) for _ in range(10)
            )
            # Setup Parameters as environment variables which are stored in a dictionary.
            export_params = (
                "python -m "
                "metaflow.plugins.airflow.plumbing.set_parameters %s "
                "&& . `pwd`/%s" % (param_file, param_file)
            )
            # Setting parameters over here.
            params = (
                entrypoint
                + top_level
                + [
                    "init",
                    self.run_id_arg,
                    "--task-id %s" % task_id_params,
                ]
            )

            # Assign tags to run objects.
            if self.tags:
                params.extend("--tag %s" % tag for tag in self.tags)

            # If the start step gets retried, we must be careful not to
            # regenerate multiple parameters tasks. Hence we check first if
            # _parameters exists already.
            exists = entrypoint + [
                # Dump the parameters task
                "dump",
                "--max-value-size=0",
                "%s/_parameters/%s" % (self.run_id, task_id_params),
            ]
            cmd = "if ! %s >/dev/null 2>/dev/null; then %s && %s; fi" % (
                " ".join(exists),
                export_params,
                " ".join(params),
            )
            cmds.append(cmd)
            # set input paths for parameters
            paths = "%s/_parameters/%s" % (self.run_id, task_id_params)

        step = [
            "step",
            node.name,
            self.run_id_arg,
            self.task_id_arg,
            "--retry-count %s" % self.attempt,
            "--max-user-code-retries %d" % user_code_retries,
            "--input-paths %s" % paths,
        ]
        if self.tags:
            step.extend("--tag %s" % tag for tag in self.tags)
        if self.namespace is not None:
            step.append("--namespace=%s" % self.namespace)
        cmds.append(" ".join(entrypoint + top_level + step))
        return cmds

    def _collect_flow_sensors(self):
        decos_lists = [
            self.flow._flow_decorators.get(s)
            for s in SUPPORTED_SENSORS
            if self.flow._flow_decorators.get(s) is not None
        ]
        af_tasks = [deco.create_task() for decos in decos_lists for deco in decos]
        if len(af_tasks) > 0:
            self._depends_on_upstream_sensors = True
        return af_tasks

    def compile(self):

        # Visit every node of the flow and recursively build the state machine.
        def _visit(node, workflow, exit_node=None):
            if node.parallel_foreach:
                raise AirflowException(
                    "Deploying flows with @parallel decorator(s) "
                    "to Airflow is not supported currently."
                )

            state = AirflowTask(node.name).set_operator_args(**self._to_job(node))

            if node.type == "end":
                workflow.add_state(state)

            # Continue linear assignment within the (sub)workflow if the node
            # doesn't branch or fork.
            elif node.type in ("start", "linear", "join"):
                workflow.add_state(state)
                _visit(
                    self.graph[node.out_funcs[0]],
                    workflow,
                )

            elif node.type == "split":
                workflow.add_state(state)
                for func in node.out_funcs:
                    _visit(
                        self.graph[func],
                        workflow,
                    )

            # We should only get here for foreach branches.
            else:
                raise AirflowException(
                    "Node type *%s* for  step *%s* "
                    "is not currently supported by "
                    "Airflow." % (node.type, node.name)
                )
            return workflow

        # set max active tasks here , For more info check here :
        # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html#airflow.models.dag.DAG
        other_args = (
            {} if self.max_workers is None else dict(max_active_tasks=self.max_workers)
        )
        other_args["is_paused_upon_creation"] = self.is_paused_upon_creation

        appending_sensors = self._collect_flow_sensors()
        workflow = Workflow(
            dag_id=self.name,
            default_args=self._create_defaults(),
            description=self.description,
            schedule_interval=self.schedule_interval,
            start_date=datetime.now(),
            tags=self.tags,
            file_path=self._file_path,
            graph_structure=self.graph_structure,
            **other_args
        )
        workflow = _visit(self.graph["start"], workflow)
        
        workflow.set_parameters(self.parameters)
        if len(appending_sensors) > 0:
            for s in appending_sensors:
                workflow.add_state(s)
            workflow.graph_structure.insert(0, [[s.name] for s in appending_sensors])
        return self._to_airflow_dag_file(workflow.to_dict())

    def _to_airflow_dag_file(self, json_dag):
        util_file = None
        with open(airflow_utils.__file__) as f:
            util_file = f.read()
        with open(AIRFLOW_DEPLOY_TEMPLATE_FILE) as f:
            return chevron.render(
                f.read(),
                dict(
                    # Converting the configuration to base64 so that there can be no indentation related issues that can be caused because of
                    # malformed strings / json.
                    metaflow_workflow_compile_params=json_dag,
                    AIRFLOW_UTILS=util_file,
                    deployed_on=str(datetime.now()),
                ),
            )

    # Not sure what is happening here. This can simply be the listed defaults in
    # the DAG template.
    def _create_defaults(self):
        defu_ = {
            "owner": get_username(),
            # If set on a task, doesn’t run the task in the current DAG run if the previous run of the task has failed.
            "depends_on_past": False,
            # TODO: Enable emails
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 0,
            "execution_timeout": timedelta(days=5),
            # check https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html?highlight=retry_delay#airflow.models.baseoperator.BaseOperatorMeta
            "retry_delay": timedelta(seconds=5),
        }
        if self.worker_pool is not None:
            defu_["pool"] = self.worker_pool

        return defu_
