from .card_modules import MetaflowCardComponent
from .card_modules.basic import ErrorComponent, SectionComponent
from .card_modules.components import UserComponent
from functools import partial
import uuid
import json
import time

_TYPE = type

# TODO move these to config
RUNTIME_CARD_MIN_REFRESH_INTERVAL = 5
RUNTIME_CARD_RENDER_INTERVAL = 60


def get_card_class(card_type):
    from metaflow.plugins import CARDS

    filtered_cards = [card for card in CARDS if card.type == card_type]
    if len(filtered_cards) == 0:
        return None
    return filtered_cards[0]


def _component_is_valid(component):
    """
    Validates if the component is of the correct class.
    """
    if not issubclass(type(component), MetaflowCardComponent):
        return False
    return True


def warning_message(message, logger=None, ts=False):
    msg = "[@card WARNING] %s" % message
    if logger:
        logger(msg, timestamp=ts, bad=True)


class WarningComponent(ErrorComponent):
    def __init__(self, warning_message):
        super().__init__("@card WARNING", warning_message)


class ComponentStore:
    """
    The `ComponentStore` class handles the in-memory storage of the components for a single card.
    This class has combination of a array/dictionary like interface to access the components.

    It exposes the `append` /`extend` methods like an array to add components.
    It also exposes the `__getitem__`/`__setitem__` methods like a dictionary to access the components by thier Ids.

    The reason this has dual behavior is because components cannot be stored entirely as a map/dictionary because
    the order of the components matter. The order of the components will visually affect the order of the components seen on the browser.

    """

    def __init__(self, logger, components=None):
        self._component_map = {}
        self._components = []
        self._logger = logger
        if components is not None:
            for c in list(components):
                self._store_component(c, component_id=None)

    def _realtime_updateable_components(self):
        for c in self._components:
            if c.REALTIME_UPDATABLE:
                yield c

    def _create_component_id(self, component):
        uuid_bit = "".join(uuid.uuid4().hex.split("-"))[:6]
        return type(component).__name__.lower() + "_" + uuid_bit

    def _store_component(self, component, component_id=None):
        if not _component_is_valid(component):
            warning_message(
                "Component (%s) is not a valid MetaflowCardComponent. It will not be stored."
                % str(component),
                self._logger,
            )
            return
        if component_id is not None:
            component.id = component_id
        elif component.id is None:
            component.id = self._create_component_id(component)
        self._components.append(component)
        self._component_map[component.id] = self._components[-1]

    def _remove_component(self, component_id):
        self._components.remove(self._component_map[component_id])
        del self._component_map[component_id]

    def __iter__(self):
        return iter(self._components)

    def __setitem__(self, key, value):
        if self._component_map.get(key) is not None:
            # FIXME: what happens to this codepath
            # Component Exists in the store
            # What happens we relace a realtime component with a non realtime one.
            # We have to ensure that layout change has take place so that the card should get re-rendered.
            pass
        else:
            self._store_component(value, component_id=key)

    def __getitem__(self, key):
        if key not in self._component_map:
            raise KeyError(
                f"MetaflowCardComponent with id `{key}` not found. Available components for the cards include : {', '.join(self.keys())}"
            )
        return self._component_map[key]

    def __delitem__(self, key):
        if key not in self._component_map:
            raise KeyError(
                f"MetaflowCardComponent with id `{key}` not found. Available components for the cards include : {', '.join(self.keys())}"
            )
        self._remove_component(key)

    def __contains__(self, key):
        return key in self._component_map

    def append(self, component, id=None):
        self._store_component(component, component_id=id)

    def extend(self, components):
        for c in components:
            self._store_component(c, component_id=None)

    def clear(self):
        self._components.clear()
        self._component_map.clear()

    def keys(self):
        return list(self._component_map.keys())

    def values(self):
        return self._components

    def __str__(self):
        return "Card components present in the card: `%s` " % ("`, `".join(self.keys()))

    def __len__(self):
        return len(self._components)


class CardComponentManager:
    """
    This class manages the components for a single card.
    It uses the `ComponentStore` to manage the storage of the components
    and exposes methods to add, remove and access the components.

    It also exposes a `refresh` method that will allow refreshing a card with new data
    for realtime(ish) updates.

    The `CardComponentCollector` class helps manage interaction with individual cards. The `CardComponentManager`
    class helps manage the components for a single card.
    The `CardComponentCollector` class uses this class to manage the components for a single card.

    The `CardComponentCollector` exposes convinience methods similar to this class for a default editable card.
    `CardComponentCollector` resolves the default editable card at the time of task initialization and
    exposes component manipulation methods for that card. These methods include :

        - `append`
        - `extend`
        - `clear`
        - `refresh`
        - `components`
        - `__iter__`

    Under the hood these common methods will call the corresponding methods on the `CardComponentManager`.
    `CardComponentManager` leverages the `ComponentStore` under the hood to actually add/update/remove components
    under the hood.

    ## Usage Patterns :

    ```python
    current.card["mycardid"].append(component, id="comp123")
    current.card["mycardid"].extend([component])
    current.card["mycardid"].refresh(data) # refreshes the card with new data
    current.card["mycardid"].components["comp123"] # returns the component with id "comp123"
    current.card["mycardid"].components["comp123"].update()
    current.card["mycardid"].components.clear() # Wipe all the components
    del current.card["mycardid"].components["mycomponentid"] # Delete a component
    current.card["mycardid"].components["mynewcomponent"] = Markdown("## New Component") # Set a new component
    ```
    """

    def __init__(
        self,
        card_uuid,
        decorator_attributes,
        card_proc,
        components=None,
        logger=None,
        no_warnings=False,
        user_set_card_id=None,
        runtime_card=False,
        card_options=None,
    ):
        self._card_proc = partial(
            card_proc,
            card_uuid,
            user_set_card_id,
            runtime_card,
            decorator_attributes,
            card_options,
            logger,
        )
        self._latest_user_data = None
        self._last_refresh = 0
        self._last_render = 0
        self._render_seq = 0
        self._logger = logger
        self._no_warnings = no_warnings
        self._warn_once = {
            "update": {},
            "not_implemented": {},
        }
        if components is None:
            self._components = ComponentStore(logger=self._logger, components=None)
        else:
            self._components = ComponentStore(
                logger=self._logger, components=list(components)
            )

    def append(self, component, id=None):
        self._components.append(component, id=id)

    def extend(self, components):
        self._components.extend(components)

    def clear(self):
        self._components.clear()

    def refresh(self, data=None, force=False):
        # todo make this a configurable variable
        self._latest_user_data = data
        nu = time.time()

        if nu - self._last_refresh < RUNTIME_CARD_MIN_REFRESH_INTERVAL:
            # rate limit refreshes: silently ignore requests that
            # happen too frequently
            return
        self._last_refresh = nu
        # FIXME force render if components have changed
        if force or nu - self._last_render > RUNTIME_CARD_RENDER_INTERVAL:
            self._render_seq += 1
            self._last_render = nu
            self._card_proc("render_runtime")
        else:
            self._card_proc("refresh")

    @property
    def components(self):
        return self._components

    def _warning(self, message):
        msg = "[@card WARNING] %s" % message
        self._logger(msg, timestamp=False, bad=True)

    def _get_latest_data(self, final=False):
        seq = "final" if final else self._render_seq
        component_dict = {}
        for component in self._components._realtime_updateable_components():
            rendered_comp = _render_card_component(component)
            if rendered_comp is not None:
                component_dict.update({component.id: rendered_comp})
        # FIXME: Verify _latest_user_data is json serializable
        return {
            "user": self._latest_user_data,
            "components": component_dict,
            "render_seq": seq,
        }

    def __iter__(self):
        return iter(self._components)


class CardComponentCollector:
    """
    This class helps collect `MetaflowCardComponent`s during runtime execution

    ### Usage with `current`
    `current.card` is of type `CardComponentCollector`

    ### Main Usage TLDR
    - [x] `current.card.append` customizes the default editable card.
    - [x] Only one card can be default editable in a step.
    - [x] The card class must have `ALLOW_USER_COMPONENTS=True` to be considered default editable.
        - [x] Classes with `ALLOW_USER_COMPONENTS=False` are never default editable.
    - [x] The user can specify an `id` argument to a card, in which case the card is editable through `current.card[id].append`.
        - [x] A card with an id can be also default editable, if there are no other cards that are eligible to be default editable.
    - [x] If multiple default-editable cards exist but only one card doesn't have an id, the card without an id is considered to be default editable.
    - [x] If we can't resolve a single default editable card through the above rules, `current.card`.append calls show a warning but the call doesn't fail.
    - [x] A card that is not default editable can be still edited through:
        - [x] its `current.card['myid']`
        - [x] by looking it up by its type, e.g. `current.card.get(type='pytorch')`.
    """

    def __init__(self, logger=None, card_proc=None):
        from metaflow.metaflow_config import CARD_NO_WARNING

        self._card_component_store = (
            # Each key in the dictionary is the UUID of an individual card.
            # value is of type `CardComponentManager`, holding a list of MetaflowCardComponents for that particular card
            {}
        )
        self._cards_meta = (
            {}
        )  # a `dict` of (card_uuid, `dict)` holding all metadata about all @card decorators on the `current` @step.
        self._card_id_map = {}  # card_id to uuid map for all cards with ids
        self._logger = logger
        self._card_proc = card_proc
        # `self._default_editable_card` holds the uuid of the card that is default editable. This card has access to `append`/`extend` methods of `self`
        self._default_editable_card = None
        self._warned_once = {
            "__getitem__": {},
            "append": False,
            "extend": False,
            "update": False,
            "update_no_id": False,
        }
        self._no_warnings = True if CARD_NO_WARNING else False

    @staticmethod
    def create_uuid():
        return str(uuid.uuid4()).replace("-", "")

    def _log(self, *args, **kwargs):
        if self._logger:
            self._logger(*args, **kwargs)

    def _add_card(
        self,
        card_type,
        card_id,
        decorator_attributes,
        card_options,
        editable=False,
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
    ):
        """
        This function helps collect cards from all the card decorators.
        As `current.card` is a singleton this function is called by all @card decorators over a @step to add editable cards.

        ## Parameters

            - `card_type` (str) : value of the associated `MetaflowCard.type`
            - `card_id` (str) : `id` argument provided at top of decorator
            - `editable` (bool) : this corresponds to the value of `MetaflowCard.ALLOW_USER_COMPONENTS` for that `card_type`
            - `customize` (bool) : This argument is reserved for a single @card decorator per @step.
                - An `editable` card with `customize=True` gets precedence to be set as default editable card.
                - A default editable card is the card which can be access via the `append` and `extend` methods.
        """
        card_uuid = self.create_uuid()
        card_metadata = dict(
            type=card_type,
            uuid=card_uuid,
            card_id=card_id,
            editable=editable,
            customize=customize,
            suppress_warnings=suppress_warnings,
            runtime_card=runtime_card,
            decorator_attributes=decorator_attributes,
            card_options=card_options,
        )
        self._cards_meta[card_uuid] = card_metadata
        self._card_component_store[card_uuid] = CardComponentManager(
            card_uuid,
            decorator_attributes,
            self._card_proc,
            components=None,
            logger=self._logger,
            no_warnings=self._no_warnings,
            user_set_card_id=card_id,
            runtime_card=runtime_card,
            card_options=card_options,
        )
        return card_metadata

    def _warning(self, message):
        msg = "[@card WARNING] %s" % message
        self._log(msg, timestamp=False, bad=True)

    def _add_warning_to_cards(self, warn_msg):
        if self._no_warnings:
            return
        for card_id in self._card_component_store:
            if not self._cards_meta[card_id]["suppress_warnings"]:
                self._card_component_store[card_id].append(WarningComponent(warn_msg))

    def get(self, type=None):
        """`get`
        gets all the components arrays for a card `type`.
        Since one `@step` can have many `@card` decorators, many decorators can have the same type. That is why this function returns a list of lists.

        Args:
            type ([str], optional): `type` of MetaflowCard. Defaults to None.

        Returns: will return empty `list` if `type` is None or not found
            List[List[MetaflowCardComponent]]
        """
        card_type = type
        card_uuids = [
            card_meta["uuid"]
            for card_meta in self._cards_meta.values()
            if card_meta["type"] == card_type
        ]
        return [self._card_component_store[uuid] for uuid in card_uuids]

    def _finalize(self):
        """
        The `_finalize` function is called once the last @card decorator calls `step_init`. Calling this function makes `current.card` ready for usage inside `@step` code.
        This function's works two parts :
        1. Resolving `self._default_editable_card`.
                - The `self._default_editable_card` holds the uuid of the card that will have access to the `append`/`extend` methods.
        2. Resolving edge cases where @card `id` argument may be `None` or have a duplicate `id` when there are more than one editable cards.
        3. Resolving the `self._default_editable_card` to the card with the`customize=True` argument.
        """
        all_card_meta = list(self._cards_meta.values())
        for c in all_card_meta:
            ct = get_card_class(c["type"])
            c["exists"] = False
            if ct is not None:
                c["exists"] = True

        # If a card has `customize=True` and is not editable then it will not be considered default editable.
        editable_cards_meta = [c for c in all_card_meta if c["editable"]]

        if len(editable_cards_meta) == 0:
            return

        # Create the `self._card_id_map` lookup table which maps card `id` to `uuid`.
        # This table has access to all cards with `id`s set to them.
        card_ids = []
        for card_meta in all_card_meta:
            if card_meta["card_id"] is not None:
                self._card_id_map[card_meta["card_id"]] = card_meta["uuid"]
                card_ids.append(card_meta["card_id"])

        # If there is only one editable card then this card becomes `self._default_editable_card`
        if len(editable_cards_meta) == 1:
            self._default_editable_card = editable_cards_meta[0]["uuid"]
            return

        # Segregate cards which have id as none and those which don't.
        not_none_id_cards = [c for c in editable_cards_meta if c["card_id"] is not None]
        none_id_cards = [c for c in editable_cards_meta if c["card_id"] is None]

        # If there is only 1 card with id set to None then we can use that as the default card.
        if len(none_id_cards) == 1:
            self._default_editable_card = none_id_cards[0]["uuid"]

        # If the size of the set of ids is not equal to total number of cards with ids then warn the user that we cannot disambiguate
        # so `current.card['my_card_id']` won't work.
        id_set = set(card_ids)
        if len(card_ids) != len(id_set):
            non_unique_ids = [
                idx
                for idx in id_set
                if len(list(filter(lambda x: x["card_id"] == idx, not_none_id_cards)))
                > 1
            ]
            nui = ", ".join(non_unique_ids)
            # throw a warning that decorators have non-unique Ids
            self._warning(
                (
                    "Multiple `@card` decorator have been annotated with duplicate ids : %s. "
                    "`current.card['%s']` will not work"
                )
                % (nui, non_unique_ids[0])
            )

            # remove the non unique ids from the `self._card_id_map`
            for idx in non_unique_ids:
                del self._card_id_map[idx]

        # if a @card has `customize=True` in the arguments then there should only be one @card with `customize=True`. This @card will be the _default_editable_card
        customize_cards = [c for c in editable_cards_meta if c["customize"]]
        if len(customize_cards) > 1:
            self._warning(
                (
                    "Multiple @card decorators have `customize=True`. "
                    "Only one @card per @step can have `customize=True`. "
                    "`current.card.append` will ignore all decorators marked `customize=True`."
                )
            )
        elif len(customize_cards) == 1:
            # since `editable_cards_meta` hold only `editable=True` by default we can just set this card here.
            self._default_editable_card = customize_cards[0]["uuid"]

    def __getitem__(self, key):
        """
        Choose a specific card for manipulation.

        When multiple @card decorators are present, you can add an
        `ID` to distinguish between them, `@card(id=ID)`. This allows you
        to add components to a specific card like this:
        ```
        current.card[ID].append(component)
        ```

        Parameters
        ----------
        key : str
            Card ID.

        Returns
        -------
        CardComponentManager
            An object with `append` and `extend` calls which allow you to
            add components to the chosen card.
        """
        if key in self._card_id_map:
            card_uuid = self._card_id_map[key]
            return self._card_component_store[card_uuid]
        if key not in self._warned_once["__getitem__"]:
            _warn_msg = [
                "`current.card['%s']` is not present. Please set the `id` argument in @card to '%s' to access `current.card['%s']`."
                % (key, key, key),
                "`current.card['%s']` will return an empty `list` which is not referenced to `current.card` object."
                % (key),
            ]
            self._warning(" ".join(_warn_msg))
            self._add_warning_to_cards("\n".join(_warn_msg))
            self._warned_once["__getitem__"][key] = True

        return []

    def __setitem__(self, key, value):
        """
        Specify components of the chosen card.

        Instead of adding components to a card individually with `current.card[ID].append(component)`,
        use this method to assign a list of components to a card, replacing the existing components:
        ```
        current.card[ID] = [FirstComponent, SecondComponent]
        ```

        Parameters
        ----------
        key: str
            Card ID.

        value: List[MetaflowCardComponent]
            List of card components to assign to this card.
        """
        if key in self._card_id_map:
            card_uuid = self._card_id_map[key]
            if not isinstance(value, list):
                _warning_msg = (
                    "`current.card['%s']` not set. `current.card['%s']` should be a `list` of `MetaflowCardComponent`."
                    % (key, key)
                )
                self._warning(_warning_msg)
                return
            self._card_component_store[card_uuid] = CardComponentManager(
                card_uuid,
                self._cards_meta[card_uuid]["decorator_attributes"],
                self._card_proc,
                components=value,
                logger=self._logger,
                no_warnings=self._no_warnings,
                user_set_card_id=key,
                card_options=self._cards_meta[card_uuid]["card_options"],
                runtime_card=self._cards_meta[card_uuid]["runtime_card"],
            )
            return

        self._warning(
            "`current.card['%s']` is not present. Please set the `id` argument in @card to '%s' to access `current.card['%s']`. "
            % (key, key, key)
        )

    def append(self, component, id=None):
        """
        Appends a component to the current card.

        Parameters
        ----------
        component : MetaflowCardComponent
            Card component to add to this card.
        """
        if self._default_editable_card is None:
            if (
                len(self._card_component_store) == 1
            ):  # if there is one card which is not the _default_editable_card then the card is not editable
                card_type = list(self._cards_meta.values())[0]["type"]
                if list(self._cards_meta.values())[0]["exists"]:
                    _crdwr = "Card of type `%s` is not an editable card." % card_type
                    _endwr = (
                        "Please use an editable card."  # todo : link to documentation
                    )
                else:
                    _crdwr = "Card of type `%s` doesn't exist." % card_type
                    _endwr = "Please use a card `type` which exits."  # todo : link to documentation

                _warning_msg = [
                    _crdwr,
                    "Component will not be appended and `current.card.append` will not work for any call during this runtime execution.",
                    _endwr,
                ]
            else:
                _warning_msg = [
                    "`current.card.append` cannot disambiguate between multiple editable cards.",
                    "Component will not be appended and `current.card.append` will not work for any call during this runtime execution.",
                    "To fix this set the `id` argument in all @card's when using multiple @card decorators over a single @step. ",  # todo : Add Link to documentation
                ]

            if not self._warned_once["append"]:
                self._warning(" ".join(_warning_msg))
                self._add_warning_to_cards("\n".join(_warning_msg))
                self._warned_once["append"] = True

            return
        self._card_component_store[self._default_editable_card].append(component, id=id)

    def extend(self, components):
        """
        Appends many components to the current card.

        Parameters
        ----------
        component : Iterator[MetaflowCardComponent]
            Card components to add to this card.
        """
        if self._default_editable_card is None:
            # if there is one card which is not the _default_editable_card then the card is not editable
            if len(self._card_component_store) == 1:
                card_type = list(self._cards_meta.values())[0]["type"]
                _warning_msg = [
                    "Card of type `%s` is not an editable card." % card_type,
                    "Components list will not be extended and `current.card.extend` will not work for any call during this runtime execution.",
                    "Please use an editable card",  # todo : link to documentation
                ]
            else:
                _warning_msg = [
                    "`current.card.extend` cannot disambiguate between multiple @card decorators.",
                    "Components list will not be extended and `current.card.extend` will not work for any call during this runtime execution.",
                    "To fix this set the `id` argument in all @card when using multiple @card decorators over a single @step.",  # todo : Add Link to documentation
                ]
            if not self._warned_once["extend"]:
                self._warning(" ".join(_warning_msg))
                self._add_warning_to_cards("\n".join(_warning_msg))
                self._warned_once["extend"] = True

            return

        self._card_component_store[self._default_editable_card].extend(components)

    @property
    def components(self):
        # FIXME: document
        if self._default_editable_card is None:
            if len(self._card_component_store) == 1:
                card_type = list(self._cards_meta.values())[0]["type"]
                _warning_msg = [
                    "Card of type `%s` is not an editable card." % card_type,
                    "Components list will not be updated and `current.card.components` will not work for any call during this runtime execution.",
                    "Please use an editable card",  # todo : link to documentation
                ]
            else:
                _warning_msg = [
                    "`current.card.components` cannot disambiguate between multiple @card decorators.",
                    "Components list will not be accessible and `current.card.components` will not work for any call during this runtime execution.",
                    "To fix this set the `id` argument in all @card when using multiple @card decorators over a single @step and reference `current.card[ID].components`",  # todo : Add Link to documentation
                    "to update/access the appropriate card component.",
                ]
            if not self._warned_once["components"]:
                self._warning(" ".join(_warning_msg))
                self._warned_once["components"] = True
            return

        return self._card_component_store[self._default_editable_card].components

    def clear(self):
        # FIXME: document
        if self._default_editable_card is not None:
            self._card_component_store[self._default_editable_card].clear()

    def refresh(self, *args, **kwargs):
        # FIXME: document
        if self._default_editable_card is not None:
            self._card_component_store[self._default_editable_card].refresh(
                *args, **kwargs
            )

    def _get_latest_data(self, card_uuid, final=False):
        """
        Returns latest data so it can be used in the final render() call
        """
        return self._card_component_store[card_uuid]._get_latest_data(final=final)

    def _serialize_components(self, card_uuid):
        """
        This method renders components present in a card to strings/json.
        Components exposed by metaflow ensure that they render safely. If components
        don't render safely then we don't add them to the final list of serialized functions
        """
        serialized_components = []
        if card_uuid not in self._card_component_store:
            return []
        has_user_components = any(
            [
                issubclass(type(component), UserComponent)
                for component in self._card_component_store[card_uuid]
            ]
        )
        for component in self._card_component_store[card_uuid]:
            rendered_obj = _render_card_component(component)
            if rendered_obj is None:
                continue
            serialized_components.append(rendered_obj)
        if has_user_components and len(serialized_components) > 0:
            serialized_components = [
                SectionComponent(contents=serialized_components).render()
            ]
        return serialized_components


def _render_card_component(component):
    if not _component_is_valid(component):
        return None
    try:
        rendered_obj = component.render()
    except:
        return None
    else:
        if not (type(rendered_obj) == str or type(rendered_obj) == dict):
            return None
        else:
            # Since `UserComponent`s are safely_rendered using render_tools.py
            # we don't need to check JSON serialization as @render_tools.render_safely
            # decorator ensures this check so there is no need to re-serialize
            if not issubclass(type(component), UserComponent):
                try:  # check if rendered object is json serializable.
                    json.dumps(rendered_obj)
                except (TypeError, OverflowError) as e:
                    return None
        return rendered_obj
