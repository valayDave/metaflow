from .card_modules import MetaflowCardComponent
from .card_modules.basic import ErrorComponent
import random
import string
import json

_TYPE = type


class SerializationErrorComponent(ErrorComponent):
    def __init__(self, component_name, error_message):
        headline = "Component %s [RENDER FAIL]" % component_name
        super().__init__(headline, error_message)


def get_card_class(card_type):
    from metaflow.plugins import CARDS

    filtered_cards = [card for card in CARDS if card.type == card_type]
    if len(filtered_cards) == 0:
        return None
    return filtered_cards[0]


class CardComponentCollector:
    """
    This class helps collect `MetaflowCardComponent`s during runtime execution

    ### Usage with `current`
    `current.cards` is of type `CardComponentCollector`

    ### Main Usage TLDR
    - [x] `current.cards.append` customizes the default editable card.
    - [x] Only one card can be default editable in a step.
    - [x] The card class must have `ALLOW_USER_COMPONENTS=True` to be considered default editable.
        - [x] Classes with `ALLOW_USER_COMPONENTS=False` are never default editable.
    - [x] The user can specify an `id` argument to a card, in which case the card is editable through `current.cards[id].append`.
        - [x] A card with an id can be also default editable, if there are no other cards that are eligible to be default editable.
    - [x] If multiple default-editable cards exist but only one card doesn’t have an id, the card without an id is considered to be default editable.
    - [x] If we can’t resolve a single default editable card through the above rules, `current.cards`.append calls show a warning but the call doesn’t fail.
    - [x] A card that is not default editable can be still edited through:
        - [x] its `current.cards['myid']`
        - [x] by looking it up by its type, e.g. `current.cards.get(type=’pytorch’)`.

    """

    def __init__(self, logger=None):
        self._cards = (
            {}
        )  # a dict with key as uuid and value as a list of MetaflowCardComponent.
        self._card_meta = (
            []
        )  # a `list` of `dict` holding all metadata about all @card decorators on the `current` @step.
        self._card_id_map = {}  # card_id to uuid map for all cards with ids
        self._logger = logger
        # `self._default_editable_card` holds the uuid of the card that is default editable. This card has access to `append`/`extend` methods of `self`
        self._default_editable_card = None

    @staticmethod
    def create_uuid():
        return "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(6)
        )

    def _log(self, *args, **kwargs):
        if self._logger:
            self._logger(*args, **kwargs)

    def _add_card(self, card_type, card_id, editable=False, customize=False):
        """
        This function helps collect cards from all the card decorators.
        As `current.cards` is a singleton this function is called by all @card decorators over a @step to add editable cards.

        ## Parameters

            - `card_type` (str) : value of the associated `MetaflowCard.type`
            - `card_id` (str) : `id` argument provided at top of decorator
            - `editable` (bool) : this corresponds to the value of `MetaflowCard.ALLOW_USER_COMPONENTS` for that `card_type`
            - `customize` (bool) : This arguement is reserved for a single @card decorator per @step.
                - An `editable` card with `customize=True` gets precidence to be set as default editable card.
                - A default editable card is the card which can be access via the `append` and `extend` methods.
        """
        card_uuid = self.create_uuid()
        card_metadata = dict(
            type=card_type,
            uuid=card_uuid,
            card_id=card_id,
            editable=editable,
            customize=customize,
        )

        self._card_meta.append(card_metadata)
        self._cards[card_uuid] = []
        return card_metadata

    def _warning(self, message):
        msg = "[@card WARNING] %s" % message
        self._log(msg, timestamp=False, bad=True)

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
            for card_meta in self._card_meta
            if card_meta["type"] == card_type
        ]
        return [self._cards[uuid] for uuid in card_uuids]

    def _finalize(self):
        """
        The `_finalize` function is called once the last @card decorator calls `step_init`. Calling this function makes `current.cards` ready for usage inside `@step` code.
        This function's works two parts :
        1. Resolving `self._default_editable_card`.
                - The `self._default_editable_card` holds the uuid of the card that will have access to the `append`/`extend` methods.
        2. Resolving edge cases where @card `id` argument may be `None` or have a duplicate `id` when there are more than one editable cards.
        3. Resolving the `self._default_editable_card` to the card with the`customize=True` argument.
        """
        all_card_meta = self._card_meta

        editable_cards_meta = [
            c
            for c in all_card_meta
            if c["editable"] or (c["customize"] and c["editable"])
        ]

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

        # Segregate cards which have id as none and those which dont.
        not_none_id_cards = [c for c in editable_cards_meta if c["card_id"] is not None]
        none_id_cards = [c for c in editable_cards_meta if c["card_id"] is None]

        # If there is only 1 card with id set to None then we can use that as the default card.
        if len(none_id_cards) == 1:
            self._default_editable_card = none_id_cards[0]["uuid"]

        # If more than one card doesn't have an `id` excluding `customize=True` card then throw warning
        elif len(none_id_cards) > 1:
            card_types = ", ".join([k["type"] for k in none_id_cards])
            warning_message = (
                "Cards of types : `%s` have `id` set to `None`. "
                "Please set `id` to each card if you wish to disambiguate using `current.cards['my_card_id']. "
            ) % card_types

            # Check if there are any cards with `customize=True`
            if any([k["customize"] for k in none_id_cards]):
                # if there is only one card left after removing ones with `customize=True` then we don't need to throw a warning as there .
                if len(none_id_cards) - 1 > 1:
                    self._warning(warning_message)
            else:
                # throw a warning that more than one card that is editable has no id set to it.
                self._warning(warning_message)

        # If the size of the set of ids is not equal to total number of cards with ids then warn the user that we cannot disambiguate
        # so `current.cards['my_card_id']` wont work.
        id_set = set(card_ids)
        if len(card_ids) != len(id_set):
            non_unique_ids = [
                idx
                for idx in id_set
                if len(list(filter(lambda x: x["card_id"] == idx, not_none_id_cards)))
                > 1
            ]
            nui = ", ".join(non_unique_ids)
            # throw a warning that decorators have non unique Ids
            self._warning(
                (
                    "Multiple `@card` decorator have been annotated with duplicate ids : %s. "
                    "`current.cards['%s']` will not work"
                )
                % (nui, non_unique_ids[0])
            )

            # remove the non unique ids from the `self._card_id_map`
            for idx in non_unique_ids:
                del self._card_id_map[idx]

        # if a @card has `customize=True` in the arguements then there should only be one @card with `customize=True`. This @card will be the _default_editable_card
        customize_cards = [c for c in editable_cards_meta if c["customize"]]
        if len(customize_cards) > 1:
            self._warning(
                (
                    "Multiple @card decorators have `customize=True`. "
                    "Only one @card per @step can have `customize=True`."
                    "All decorators marked `customize=True`"
                )
            )
        elif len(customize_cards) == 1:
            # since `editable_cards_meta` hold only `editable=True` by default we can just set this card here.
            self._default_editable_card = customize_cards[0]["uuid"]

    def __getitem__(self, key):
        if key in self._card_id_map:
            card_uuid = self._card_id_map[key]
            return self._cards[card_uuid]

        self._warning(
            (
                "`current.cards['%s']` is not present. Please set the `id` argument in @card to '%s' to access `current.cards['%s']`. "
                "`current.cards['%s']` will return an empty `list` which is not referenced to `current.cards` object."
            )
            % (key, key, key, key)
        )
        return []

    def __setitem__(self, key, value):
        if key in self._card_id_map:
            card_uuid = self._card_id_map[key]
            if not isinstance(value, list):
                self._warning(
                    "`current.cards['%s']` not set. `current.cards['%s']` should be a `list` of `MetaflowCardComponent`."
                    % (key, key)
                )
                return
            self._cards[card_uuid] = value
            return

        self._warning(
            "`current.cards['%s']` is not present. Please set the `id` argument in @card to '%s' to access `current.cards['%s']`. "
            % (key, key, key)
        )

    def append(self, component):
        if self._default_editable_card is None:
            if (
                len(self._cards) == 1
            ):  # if there is one card which is not the _default_editable_card then the card is not editable
                card_type = self._card_meta[0]["type"]
                self._warning(
                    (
                        "Card of type `%s` is not an editable card. "
                        "Component will not be appended. "
                        "Please use an editable card. "  # todo : link to documentation
                    )
                    % card_type
                )
            else:
                self._warning(
                    (
                        "`current.cards.append` cannot disambiguate between multiple editable cards. "
                        "Component will not be appended. "
                        "To fix this set the `id` argument in all @card's when using multiple @card decorators over a single @step. "  # todo : Add Link to documentation
                    )
                )
            return
        self._cards[self._default_editable_card].append(component)

    def extend(self, components):
        if self._default_editable_card is None:
            if (
                len(self._cards) == 1
            ):  # if there is one card which is not the _default_editable_card then the card is not editable
                card_type = self._card_meta[0]["type"]
                self._warning(
                    (
                        "Card of type `%s` is not an editable card."
                        "Component will not be extended. "
                        "Please use an editable card"  # todo : link to documentation
                    )
                    % card_type
                )
            else:
                self._warning(
                    (
                        "`current.cards.extend` cannot disambiguate between multiple @card decorators. "
                        "Component will not be extended. "
                        "To fix this set the `id` argument in all @card when using multiple @card decorators over a single @step. "  # todo : Add Link to documentation
                    )
                )
            return

        self._cards[self._default_editable_card].extend(components)

    def _serialize_components(self, card_uuid):
        import traceback

        serialized_components = []
        if card_uuid not in self._cards:
            return []
        for component in self._cards[card_uuid]:
            if not issubclass(type(component), MetaflowCardComponent):
                continue
            try:
                rendered_obj = component.render()
            except:
                error_str = traceback.format_exc()
                serialized_components.append(
                    SerializationErrorComponent(
                        component.__class__.__name__, error_str
                    ).render()
                )
            else:
                if not (type(rendered_obj) == str or type(rendered_obj) == dict):
                    rendered_obj = SerializationErrorComponent(
                        component.__class__.__name__,
                        "Component render didn't return a `string` or `dict`",
                    ).render()
                else:
                    try:  # check if rendered object is json serializable.
                        json.dumps(rendered_obj)
                    except (TypeError, OverflowError):
                        rendered_obj = SerializationErrorComponent(
                            component.__class__.__name__,
                            "Rendered Component cannot be JSON serialized.",
                        ).render()
                serialized_components.append(rendered_obj)
        return serialized_components