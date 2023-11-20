from typing import Any, List, Optional, Union
from .basic import (
    LogComponent,
    ErrorComponent,
    ArtifactsComponent,
    TableComponent,
    ImageComponent,
    SectionComponent,
    MarkdownComponent,
)
from .card import MetaflowCardComponent
from .convert_to_native_type import TaskToDict, _full_classname
from .renderer_tools import render_safely
import uuid


def create_component_id(component):
    uuid_bit = "".join(uuid.uuid4().hex.split("-"))[:6]
    return type(component).__name__.lower() + "_" + uuid_bit


def with_default_component_id(func):
    def ret_func(self, *args, **kwargs):
        if self.component_id is None:
            self.component_id = create_component_id(self)
        return func(self, *args, **kwargs)

    return ret_func


def _warning_with_component(component, msg):
    if component._logger is None:
        return None
    if component._warned_once:
        return None
    log_msg = "[@card-component WARNING] %s" % msg
    component._logger(log_msg, timestamp=False, bad=True)
    component._warned_once = True


class UserComponent(MetaflowCardComponent):

    _warned_once = False

    def update(self, *args, **kwargs):
        cls_name = self.__class__.__name__
        msg = (
            "MetaflowCardComponent doesn't have an `update` method implemented "
            "and is not compatible with realtime updates."
        ) % cls_name
        _warning_with_component(self, msg)


class StubComponent(UserComponent):
    def __init__(self, component_id):
        self._non_existing_comp_id = component_id

    def update(self, *args, **kwargs):
        msg = "Component with id %s doesn't exist. No updates will be made at anytime during runtime."
        _warning_with_component(self, msg % self._non_existing_comp_id)


class Artifact(UserComponent):
    """
    A pretty-printed version of any Python object.

    Large objects are truncated using Python's built-in [`reprlib`](https://docs.python.org/3/library/reprlib.html).

    Example:
    ```
    from datetime import datetime
    current.card.append(Artifact({'now': datetime.utcnow()}))
    ```

    Parameters
    ----------
    artifact : object
        Any Python object.
    name : str, optional
        Optional label for the object.
    compressed : bool, default: True
        Use a truncated representation.
    """

    REALTIME_UPDATABLE = True

    def update(self, artifact):
        self._artifact = artifact

    def __init__(
        self, artifact: Any, name: Optional[str] = None, compressed: bool = True
    ):
        self._artifact = artifact
        self._name = name
        self._task_to_dict = TaskToDict(only_repr=compressed)

    @with_default_component_id
    @render_safely
    def render(self):
        artifact = self._task_to_dict.infer_object(self._artifact)
        artifact["name"] = None
        if self._name is not None:
            artifact["name"] = str(self._name)
        af_component = ArtifactsComponent(data=[artifact])
        af_component.component_id = self.component_id
        return af_component.render()


class Table(UserComponent):
    """
    A table.

    The contents of the table can be text or numerical data, a Pandas dataframe,
    or other card components: `Artifact`, `Image`, `Markdown` objects.

    Example: Text and artifacts
    ```
    from metaflow.cards import Table, Artifact
    current.card.append(
        Table([
            ['first row', Artifact({'a': 2})],
            ['second row', Artifact(3)]
        ])
    )
    ```

    Example: Table from a Pandas dataframe
    ```
    from metaflow.cards import Table
    import pandas as pd
    import numpy as np
    current.card.append(
        Table.from_dataframe(
            pd.DataFrame(
                np.random.randint(0, 100, size=(15, 4)),
                columns=list("ABCD")
            )
        )
    )
    ```

    Parameters
    ----------
    data : List[List[str or MetaflowCardComponent]], optional
        List (rows) of lists (columns). Each item can be a string or a `MetaflowCardComponent`.
    headers : List[str], optional
        Optional header row for the table.
    disable_updates: bool, optional
        A boolean value to disable realtime updates for all components within the table. Default: False
    """

    REALTIME_UPDATABLE = True

    def update(self, *args, **kwargs):
        msg = (
            "`Table` doesn't have an `update` method implemented. "
            "Components within a table can be updated individually "
            "but the table itself cannot be updated."
        )
        _warning_with_component(self, msg)

    def __init__(
        self,
        data: Optional[List[List[Union[str, MetaflowCardComponent]]]] = None,
        headers: Optional[List[str]] = None,
        disable_updates: bool = False,
    ):
        data = data or [[]]
        headers = headers or []
        header_bool, data_bool = TableComponent.validate(headers, data)
        self._headers = []
        self._data = [[]]
        if header_bool:
            self._headers = headers
        if data_bool:
            self._data = data

        if disable_updates:
            self.REALTIME_UPDATABLE = False

    @classmethod
    def from_dataframe(
        cls,
        dataframe=None,
        truncate: bool = True,
    ):
        """
        Create a `Table` based on a Pandas dataframe.

        Parameters
        ----------
        dataframe : Optional[pandas.DataFrame]
            Pandas dataframe.
        truncate : bool, default: True
            Truncate large dataframe instead of showing all rows (default: True).
        """
        task_to_dict = TaskToDict()
        object_type = task_to_dict.object_type(dataframe)
        if object_type == "pandas.core.frame.DataFrame":
            table_data = task_to_dict._parse_pandas_dataframe(
                dataframe, truncate=truncate
            )
            return_val = cls(
                data=table_data["data"],
                headers=table_data["headers"],
                disable_updates=True,
            )
            return return_val
        else:
            return cls(
                headers=["Object type %s not supported" % object_type],
                disable_updates=True,
            )

    def _render_subcomponents(self):
        return [
            SectionComponent.render_subcomponents(
                row,
                additional_allowed_types=[
                    str,
                    bool,
                    int,
                    float,
                    dict,
                    list,
                    tuple,
                    type(None),
                ],
                allow_unknowns=True,
            )
            for row in self._data
        ]

    @with_default_component_id
    @render_safely
    def render(self):
        table_component = TableComponent(
            headers=self._headers, data=self._render_subcomponents()
        )
        table_component.component_id = self.component_id
        return table_component.render()


class Image(UserComponent):
    """
    An image.

    `Images can be created directly from PNG/JPG/GIF `bytes`, `PIL.Image`s,
    or Matplotlib figures. Note that the image data is embedded in the card,
    so no external files are required to show the image.

    Example: Create an `Image` from bytes:
    ```
    current.card.append(
        Image(
            requests.get("https://www.gif-vif.com/hacker-cat.gif").content,
            "Image From Bytes"
        )
    )
    ```

    Example: Create an `Image` from a Matplotlib figure
    ```
    import pandas as pd
    import numpy as np
    current.card.append(
        Image.from_matplotlib(
            pandas.DataFrame(
                np.random.randint(0, 100, size=(15, 4)),
                columns=list("ABCD"),
            ).plot()
        )
    )
    ```

    Example: Create an `Image` from a [PIL](https://pillow.readthedocs.io/) Image
    ```
    from PIL import Image as PILImage
    current.card.append(
        Image.from_pil_image(
            PILImage.fromarray(np.random.randn(1024, 768), "RGB"),
            "From PIL Image"
        )
    )
    ```

    Parameters
    ----------
    src : bytes
        The image data in `bytes`.
    label : str
        Optional label for the image.
    disable_updates: bool
        Disable realtime updates for the image. Default: True
    """

    REALTIME_UPDATABLE = True

    @staticmethod
    def render_fail_headline(msg):
        return "[IMAGE_RENDER FAIL]: %s" % msg

    def _set_image_src(self, src, label=None):
        self._label = label
        self._src = None
        self._error_comp = None
        if src is None:
            self._error_comp = ErrorComponent(
                self.render_fail_headline("`Image` Component `src` cannot be `None`"),
                "",
            )
        elif type(src) is not str:
            try:
                self._src = self._bytes_to_base64(src)
            except TypeError:
                self._error_comp = ErrorComponent(
                    self.render_fail_headline(
                        "The `Image` `src` argument should be of type `bytes` or valid image base64 string"
                    ),
                    "Type of %s is invalid" % (str(type(src))),
                )
            except ValueError:
                self._error_comp = ErrorComponent(
                    self.render_fail_headline("Bytes not parsable as image"), ""
                )
            except Exception as e:
                import traceback

                self._error_comp = ErrorComponent(
                    self.render_fail_headline("Bytes not parsable as image"),
                    "%s\n\n%s" % (str(e), traceback.format_exc()),
                )
        else:
            if "data:image/" in src:
                self._src = src
            else:
                self._error_comp = ErrorComponent(
                    self.render_fail_headline(
                        "The `Image` `src` argument should be of type `bytes` or valid image base64 string"
                    ),
                    "String %s is invalid base64 string" % src,
                )

    def __init__(self, src=None, label=None, disable_updates: bool = True):
        if disable_updates:
            self.REALTIME_UPDATABLE = False
        self._set_image_src(src, label=label)

    @classmethod
    def _parse_pil_image(cls, pilimage):
        parsed_value = None
        error_component = None
        import io

        PIL_IMAGE_PATH = "PIL.Image.Image"
        task_to_dict = TaskToDict()
        if task_to_dict.object_type(pilimage) != PIL_IMAGE_PATH:
            return parsed_value, ErrorComponent(
                cls.render_fail_headline(
                    "first argument for `Image` should be of type %s" % PIL_IMAGE_PATH
                ),
                "Type of %s is invalid. Type of %s required"
                % (task_to_dict.object_type(pilimage), PIL_IMAGE_PATH),
            )
        img_byte_arr = io.BytesIO()
        try:
            pilimage.save(img_byte_arr, format="PNG")
        except OSError as e:
            return parsed_value, ErrorComponent(
                cls.render_fail_headline("PIL Image Not Parsable"), "%s" % repr(e)
            )
        img_byte_arr = img_byte_arr.getvalue()
        parsed_value = task_to_dict.parse_image(img_byte_arr)
        return parsed_value, error_component

    @classmethod
    def _parse_matplotlib(cls, plot):
        import io
        import traceback

        parsed_value = None
        error_component = None
        try:
            import matplotlib.pyplot as pyplt
        except ImportError:
            return parsed_value, ErrorComponent(
                cls.render_fail_headline("Matplotlib cannot be imported"),
                "%s" % traceback.format_exc(),
            )
        # First check if it is a valid Matplotlib figure.
        figure = None
        if _full_classname(plot) == "matplotlib.figure.Figure":
            figure = plot

        # If it is not valid figure then check if it is matplotlib.axes.Axes or a matplotlib.axes._subplots.AxesSubplot
        # These contain the `get_figure` function to get the main figure object.
        if figure is None:
            if getattr(plot, "get_figure", None) is None:
                return parsed_value, ErrorComponent(
                    cls.render_fail_headline(
                        "Invalid Type. Object %s is not from `matplotlib`" % type(plot)
                    ),
                    "",
                )
            else:
                figure = plot.get_figure()

        task_to_dict = TaskToDict()
        img_bytes_arr = io.BytesIO()
        figure.savefig(img_bytes_arr, format="PNG")
        parsed_value = task_to_dict.parse_image(img_bytes_arr.getvalue())
        pyplt.close(figure)
        if parsed_value is not None:
            return parsed_value, error_component
        return parsed_value, ErrorComponent(
            cls.render_fail_headline("Matplotlib plot's image is not parsable"), ""
        )

    @staticmethod
    def _bytes_to_base64(bytes_arr):
        task_to_dict = TaskToDict()
        if task_to_dict.object_type(bytes_arr) != "bytes":
            raise TypeError
        parsed_image = task_to_dict.parse_image(bytes_arr)
        if parsed_image is None:
            raise ValueError
        return parsed_image

    @classmethod
    def from_pil_image(cls, pilimage, label: Optional[str] = None):
        """
        Create an `Image` from a PIL image.

        Parameters
        ----------
        pilimage : PIL.Image
            a PIL image object.
        label : str, optional
            Optional label for the image.
        """
        try:
            parsed_image, error_comp = cls._parse_pil_image(pilimage)
            if parsed_image is not None:
                img = cls(src=parsed_image, label=label)
            else:
                img = cls(src=None, label=label)
                img._error_comp = error_comp
            return img
        except:
            import traceback

            img = cls(src=None, label=label)
            img._error_comp = ErrorComponent(
                cls.render_fail_headline("PIL Image Not Parsable"),
                "%s" % traceback.format_exc(),
            )
            return img

    @classmethod
    def from_matplotlib(cls, plot, label: Optional[str] = None):
        """
        Create an `Image` from a Matplotlib plot.

        Parameters
        ----------
        plot :  matplotlib.figure.Figure or matplotlib.axes.Axes or matplotlib.axes._subplots.AxesSubplot
            a PIL axes (plot) object.
        label : str, optional
            Optional label for the image.
        """
        try:
            parsed_image, error_comp = cls._parse_matplotlib(plot)
            if parsed_image is not None:
                img = cls(src=parsed_image, label=label)
            else:
                img = cls(src=None, label=label)
                img._error_comp = error_comp
            return img
        except:
            import traceback

            img = cls(src=None, label=label)
            img._error_comp = ErrorComponent(
                cls.render_fail_headline("Matplotlib plot's image is not parsable"),
                "%s" % traceback.format_exc(),
            )
            return img

    @with_default_component_id
    @render_safely
    def render(self):
        if self._error_comp is not None:
            return self._error_comp.render()

        if self._src is not None:
            img_comp = ImageComponent(src=self._src, label=self._label)
            img_comp.component_id = self.component_id
            return img_comp.render()
        return ErrorComponent(
            self.render_fail_headline("`Image` Component `src` argument is `None`"), ""
        ).render()

    def update(self, pilimage=None, plot=None, bytes=None, string=None, label=None):
        """
        Update the image.

        Parameters
        ----------
        pilimage : PIL.Image, optional
            a PIL image object.
        plot :  matplotlib.figure.Figure or matplotlib.axes.Axes or matplotlib.axes._subplots.AxesSubplot, optional
            a PIL axes (plot) object.
        bytes : bytes, optional
            The image data in `bytes`.
        string : str, optional
            The image data in base64 string.
        label : str, optional
            Optional label for the image.
        """
        if not self.REALTIME_UPDATABLE:
            msg = (
                "The `Image` component is disabled for realtime updates. "
                "Please set `disable_updates` to `False` while creating the `Image` object."
            )
            _warning_with_component(self, msg)
            return

        _label = label if label is not None else self._label
        if bytes is not None:
            self._set_image_src(bytes, label=_label)
            return
        elif string is not None:
            self._set_image_src(string, label=_label)
            return

        parsed_image = None
        err_comp = None
        if pilimage is not None:
            parsed_image, err_comp = self._parse_pil_image(pilimage)
        elif plot is not None:
            parsed_image, err_comp = self._parse_matplotlib(plot)

        if parsed_image is not None:
            self._set_image_src(parsed_image, label=_label)
        else:
            self._set_image_src(None, label=_label)
            self._error_comp = err_comp


class Error(UserComponent):
    """
    This class helps visualize Error's on the `MetaflowCard`. It can help catch and print stack traces to errors that happen in `@step` code.

    ### Parameters
    - `exception` (Exception) : The `Exception` to visualize. This value will be `repr`'d before passed down to `MetaflowCard`
    - `title` (str) : The title that will appear over the visualized  `Exception`.

    ### Usage
    ```python
    @card
    @step
    def my_step(self):
        from metaflow.cards import Error
        from metaflow import current
        try:
            ...
            ...
        except Exception as e:
            current.card.append(
                Error(e,"Something misbehaved")
            )
        ...
    ```
    """

    def __init__(self, exception, title=None):
        self._exception = exception
        self._title = title

    @render_safely
    def render(self):
        return LogComponent("%s\n\n%s" % (self._title, repr(self._exception))).render()


class Markdown(UserComponent):
    """
    A block of text formatted in Markdown.

    Example:
    ```
    current.card.append(
        Markdown("# This is a header appended from `@step` code")
    )
    ```

    Parameters
    ----------
    text : str
        Text formatted in Markdown.
    """

    REALTIME_UPDATABLE = True

    def update(self, text=None):
        self._text = text

    def __init__(self, text=None):
        self._text = text

    @with_default_component_id
    @render_safely
    def render(self):
        comp = MarkdownComponent(self._text)
        comp.component_id = self.component_id
        return comp.render()


class ProgressBar(UserComponent):
    type = "progressBar"

    REALTIME_UPDATABLE = True

    def __init__(self, max=100, label=None, value=0, unit=None, metadata=None):
        self._label = label
        self._max = max
        self._value = value
        self._unit = unit
        self._metadata = metadata

    def update(self, new_value, metadata=None):
        self._value = new_value
        if metadata is not None:
            self._metadata = metadata

    @with_default_component_id
    @render_safely
    def render(self):
        data = {
            "type": self.type,
            "id": self.component_id,
            "max": self._max,
            "value": self._value,
        }
        if self._label:
            data["label"] = self._label
        if self._unit:
            data["unit"] = self._unit
        if self._metadata:
            data["details"] = self._metadata
        return data


class VegaChart(UserComponent):
    type = "vegaChart"

    REALTIME_UPDATABLE = True

    def __init__(self, spec, data=None):
        self._spec = spec
        self._data = data

    def update(self, data=None, spec=None):
        if spec is not None:
            self._spec = spec
        if data is not None:
            self._data = data

    @classmethod
    def from_altair_chart(cls, altair_chart):
        from metaflow.plugins.cards.card_modules.convert_to_native_type import (
            _full_classname,
        )

        # This will feel slightly hacky but I am unable to find a natural way of determining the class
        # name of the Altair chart. The only way I can think of is to use the full class name and then
        # match with heuristics

        fulclsname = _full_classname(altair_chart)
        if not all([x in fulclsname for x in ["altair", "vegalite", "Chart"]]):
            raise ValueError(fulclsname + " is not an altair chart")

        altair_chart_dict = altair_chart.to_dict()

        data_object = None
        if "datasets" in altair_chart_dict:
            data_object = altair_chart_dict["datasets"]
            del altair_chart_dict["datasets"]
        return cls(spec=altair_chart_dict, data=data_object)

    @with_default_component_id
    @render_safely
    def render(self):
        data = {
            "type": self.type,
            "id": self.component_id,
            "spec": self._spec,
            "data": self._data,
        }
        return data
