import datetime
from collections import namedtuple
from .basic import BlankCard, MarkdownComponent, DefaultComponent, ErrorComponent
from .components import Section

ProfilerReading = namedtuple("ProfilerReading", ["time", "cpu", "memory", "disk"])

TIME_FORMAT = "%Y-%m-%d %I:%M:%S %p"


class ChartComponent(DefaultComponent):
    def __init__(
        self,
        chart_config=None,
        data=[[]],
        labels=[],
    ):
        super().__init__(title=None, subtitle=None)
        self._chart_config = chart_config
        self._data = data
        self._labels = labels
        # We either use data & labels OR chart_config
        # the chart_config object is a

    def render(self):
        render_dict = super().render()
        if self._chart_config is not None:
            render_dict["config"] = self._chart_config
            return render_dict
        # No `chart_config` is provided.
        # Since there is no `chart_config` we pass the `data` and `labels` object.
        render_dict.update(dict(data=self._data, labels=self._labels))
        return render_dict


class LineChartComponent(ChartComponent):
    type = "lineChart"

    def __init__(self, chart_config=None, data=[], labels=[]):
        super().__init__(chart_config=chart_config, data=data, labels=labels)


class BarChartComponent(ChartComponent):
    type = "barChart"

    def __init__(self, chart_config=None, data=[[]], labels=[]):
        super().__init__(chart_config=chart_config, data=data, labels=labels)


class ProfilingCard(BlankCard):

    profile = []

    periodic = True

    min_period = 1  # in seconds

    buffer_size = 300

    ALLOW_USER_COMPONENTS = False

    measuring_since = None

    type = "profiling_card"

    CORE_MARKDOWN = """
# Profiling Card (%s)
_Measured At : %s_

"""

    @staticmethod
    def _get_ps_util():
        try:
            import psutil

            return psutil
        except ImportError:
            return None

    @classmethod
    def periodic_render(cls):
        if not cls._get_ps_util():
            return [
                ErrorComponent(
                    "psutil is absent",
                    "Cannot take CPU and Memory Profiling Readings. `psutil` needs to be present",
                ).render()
            ]
        latest_reading = cls._get_readings()
        # This means first measurement
        if len(cls.profile) == 0:
            cls.measuring_since = latest_reading.time
        else:
            # Only retun profile if it was called after `min_period`
            time_diff = latest_reading.time - cls.profile[-1].time
            if time_diff.seconds < cls.min_period:
                return []

        cls.profile.append(latest_reading)
        # Discard old data
        if len(cls.profile) > cls.buffer_size:
            cls.profile = cls.profile[1:]
        measuring_since = (
            ""
            if cls.measuring_since is None
            else "_Measuring Since_ : %s" % cls.measuring_since.strftime(TIME_FORMAT)
        )
        ms = []
        if measuring_since != "":
            ms = [MarkdownComponent(measuring_since).render()]
        return ms + [
            Section("% CPU usage over time").render(),
            LineChartComponent(
                data=[p.cpu for p in cls.profile],
                labels=[p.time.strftime(TIME_FORMAT) for p in cls.profile],
            ).render(),
            Section("% Memory usage over time").render(),
            LineChartComponent(
                data=[p.memory for p in cls.profile],
                labels=[p.time.strftime(TIME_FORMAT) for p in cls.profile],
            ).render(),
            Section("% Disk usage over time").render(),
            LineChartComponent(
                data=[p.disk for p in cls.profile],
                labels=[p.time.strftime(TIME_FORMAT) for p in cls.profile],
            ).render(),
        ]

    @classmethod
    def _get_readings(cls):
        psutil = cls._get_ps_util()
        return ProfilerReading(
            datetime.datetime.now(),
            psutil.cpu_percent(),
            psutil.virtual_memory().percent,
            psutil.disk_usage("/").percent,
        )

    def __init__(self, options={}, components=[], graph=None):
        super().__init__(
            options=dict(title="Profiling Card"), components=components, graph=graph
        )

    def render(self, task):
        measurement = MarkdownComponent(
            self.CORE_MARKDOWN
            % (
                task.pathspec,
                datetime.datetime.now().strftime(TIME_FORMAT),
            )
        )
        return super().render(task, components=[measurement])
