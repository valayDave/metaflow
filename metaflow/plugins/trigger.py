import os
from metaflow.decorators import FlowDecorator


class TriggerDecorator(FlowDecorator):
    name = "trigger"
    defaults = {"event": None}

    def __init__(self, *args, statically_defined=False, **kwargs):
        self._kwargs = kwargs
        _defaults = {
            k: self._kwargs[k] if k in self._kwargs else v
            for k, v in self.defaults.items()
        }
        super().__init__(
            *args,
            attributes=_defaults,
            statically_defined=statically_defined,
        )
        self.attributes.update(self._kwargs["attributes"])


class TriggerOnFinishDecorator(TriggerDecorator):
    name = "trigger_on_finish"
    defaults = {
        "flow": None,
        "branch": None,
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        from metaflow import Run

        flow.input_flow = None
        tags = []
        if "METAFLOW_UPSTREAM_EVENT_TRIGGER_PATHSPEC" in os.environ:
            pth = "/".join(
                os.environ["METAFLOW_UPSTREAM_EVENT_TRIGGER_PATHSPEC"].split("/")[:2]
            )
            flow.input_flow = Run(pth)
            tags.append("parent_run_pathspec:%s" % pth)
        if "METAFLOW_EVENTS_BACKEND" in os.environ:
            backend = os.environ["METAFLOW_EVENTS_BACKEND"]
            tags.append("events_backend:%s" % backend)
        if len(tags) > 0:
            metadata.add_sticky_tags(sys_tags=tags)
