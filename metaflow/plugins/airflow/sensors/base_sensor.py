from metaflow.decorators import FlowDecorator
from ..exception import AirflowException
from ..airflow_utils import AirflowTask


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
        # TODO : (savin-comments) : refactor the name of `self._task_name` to have a common name.
        # Is the task is task name a metaflow task?
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
                # TODO : (savin-comments)  autogenerate this name
                raise AirflowException(
                    "`name` argument cannot be `None` when multiple Airflow Sensor related decorators are attached to a flow."
                )
        if self.attributes["name"] is not None:
            self._task_name = self.attributes["name"]

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        self.compile()
