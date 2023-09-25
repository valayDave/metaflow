from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
import os
import hashlib
import shutil
import tempfile
import json

CARD_STATE_ROOT_DIR = os.path.join(DATASTORE_LOCAL_DIR, "mf.card_state")


class StateContainer:
    def __init__(
        self,
        file_name,
    ):
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        self._file_name = file_name
        self._file_created = os.path.exists(self._file_name)

    @property
    def is_present(self):
        return os.path.exists(self._file_name)

    def load(self):
        with open(self._file_name, "r") as _file:
            state = json.load(_file)
        return state

    def save(self, state):
        with open(self._file_name, "w") as _file:
            json.dump(state, _file)
        self._file_created = True

    def done(self):
        if self._file_created:
            os.remove(self._file_name)


class CardStateManager:

    card_state = {}

    def __init__(self, pathspec) -> None:
        self._pathspec = pathspec
        self._card_state_directory = _get_card_state_directory(pathspec)
        self._component_collector_state = StateContainer(
            _get_card_collector_state_file_path(pathspec)
        )
        self._state_file_created = False

    @property
    def component_collector(self):
        return self._component_collector_state

    def done(self):
        if self._state_file_created:
            self._component_collector_state.done()
            shutil.rmtree(self._card_state_directory)

    def _save_card_state(self, uuid, components=None, data=None):
        if uuid not in self.card_state:
            self.card_state[uuid] = {
                "components": StateContainer(
                    os.path.join(self._card_state_directory, f"{uuid}.components.json")
                ),
                "data": StateContainer(
                    os.path.join(self._card_state_directory, f"{uuid}.data.json")
                ),
            }
        if components is not None:
            self.card_state[uuid]["components"].save(components)
        if data is not None:
            self.card_state[uuid]["data"].save(data)


def _get_card_collector_state_file_path(pathspec):
    return os.path.join(_get_card_state_directory(pathspec), "card_state.json")


def _get_card_state_directory(pathspec):
    pathspec_hash = hashlib.md5(pathspec.encode()).hexdigest()
    return os.path.join(CARD_STATE_ROOT_DIR, pathspec_hash)


def get_realtime_cards(
    pathspec=None,
):
    # FIXME : What is a way we can gaurentee that `CardStateManager`
    # Loads the correct path based on what ever it's CWD is set in the subprocess
    # calling this.
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
        state_dict=card_state_manager.component_collector.load(),
        logger=logger,
        state_manager=card_state_manager,
    )
