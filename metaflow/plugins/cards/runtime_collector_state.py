from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
import os
import hashlib
import tempfile
import json

CARD_STATE_ROOT_DIR = os.path.join(DATASTORE_LOCAL_DIR, "mf.card_state")


class CardStateManager:
    def __init__(self, pathspec) -> None:
        self._pathspec = pathspec
        # Create a directory that will store the card state.
        os.makedirs(CARD_STATE_ROOT_DIR, exist_ok=True)
        self._card_state_directory = _get_card_state_directory(pathspec)
        self._card_state_file = _get_card_state_file(pathspec)
        self._state_file_created = False

    def load(self):
        with open(self._card_state_file, "r") as _state_file:
            state = json.load(_state_file)
        return state

    def save(self, state):
        os.makedirs(self._card_state_directory, exist_ok=True)
        _state_file = open(self._card_state_file, "w")
        json.dump(state, _state_file)
        _state_file.close()
        self._state_file_created = True

    def done(self):
        if self._state_file_created:
            os.remove(self._card_state_file)
            os.rmdir(self._card_state_directory)


def _get_card_state_file(pathspec):
    return os.path.join(_get_card_state_directory(pathspec), "card_state.json")


def _get_card_state_directory(pathspec):
    pathspec_hash = hashlib.md5(pathspec.encode()).hexdigest()
    return os.path.join(CARD_STATE_ROOT_DIR, pathspec_hash)


def get_realtime_cards(
    pathspec=None,
):
    from .component_serializer import CardComponentCollector
    from metaflow import current
    from metaflow.cli import logger

    if pathspec is None:
        # if getattr(current, "card", None):
        #     return current.card
        # else:
        if "METAFLOW_CARD_PATHSPEC" not in os.environ:
            raise ValueError(
                "It seems like `get_realtime_cards` is being called from outside a Metaflow step process. "
                "In such cases a `pathspec` argument is required or `METAFLOW_CARD_PATHSPEC` needs to be set "
                "as an environment variable."
            )
        pathspec = os.environ["METAFLOW_CARD_PATHSPEC"]

    card_state_manager = CardStateManager(pathspec)
    return CardComponentCollector._load_state(
        state_dict=card_state_manager.load(), logger=logger
    )
