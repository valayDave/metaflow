""" 

"""

from collections import namedtuple
from hashlib import sha1
from io import BytesIO
import os
import shutil

from metaflow.datastore.local_storage import LocalStorage
from metaflow.metaflow_config import DATASTORE_CARD_S3ROOT, DATASTORE_CARD_LOCALROOT

from .exception import CardNotPresentException

TEMP_DIR_NAME = "metaflow_card_cache"
NUM_SHORT_HASH_CHARS = 5

CardInfo = namedtuple("CardInfo", ["type", "hash"])


def path_spec_resolver(pathspec):
    run_id, step_name, task_id = None, None, None
    splits = pathspec.split("/")
    if len(splits) == 1:  # only flowname mentioned
        return splits[0], run_id, step_name, task_id
    elif len(splits) == 2:  # flowname , runid mentioned
        return splits[0], splits[1], step_name, task_id
    elif len(splits) == 3:  # flowname , runid , stepname
        return splits[0], splits[1], splits[2], task_id
    elif len(splits) == 4:  # flowname ,runid ,stepname , taskid
        return splits[0], splits[1], splits[2], splits[3]


class CardDatastore(object):
    @classmethod
    def get_storage_root(cls, storage_type):
        if storage_type == "s3":
            return DATASTORE_CARD_S3ROOT
        else:
            return DATASTORE_CARD_LOCALROOT

    def __init__(self, flow_datastore, run_id, step_name, task_id, path_spec=None):
        self._backend = flow_datastore._storage_impl
        self._flow_name = flow_datastore.flow_name
        self._run_id = run_id
        self._step_name = step_name
        self._task_id = task_id
        self._pathspec = path_spec
        self._temp_card_save_path = self._get_card_path(base_pth=TEMP_DIR_NAME)
        LocalStorage._makedirs(self._temp_card_save_path)

    @classmethod
    def get_card_location(cls, base_path, card_name, card_html):
        return os.path.join(
            base_path,
            "%s-%s.html" % (card_name, sha1(bytes(card_html, "utf-8")).hexdigest()),
        )

    def _make_path(self, base_pth, pathspec=None):
        sysroot = base_pth

        if pathspec is not None:
            flow_name, run_id, step_name, task_id = path_spec_resolver(pathspec)

        # For task level cards the flow_name and run_id and task_id are required
        if flow_name is not None and run_id is not None and task_id is not None:
            pth_arr = [
                sysroot,
                flow_name,
                "runs",
                run_id,
                "tasks",
                task_id,
                "cards",
            ]

        if sysroot == "" or sysroot == None:
            pth_arr.pop(0)
        return self._backend.path_join(*pth_arr)

    def _get_card_path(self, base_pth=""):
        return self._make_path(
            base_pth,
            pathspec=self._pathspec,
        )

    @staticmethod
    def card_info_from_path(path):
        """
        Args:
            path (str): The path to the card

        Raises:
            Exception: When the card_path is invalid

        Returns:
            CardInfo
        """
        card_file_name = path.split("/")[-1]
        file_split = card_file_name.split("-")
        if len(file_split) != 2:
            raise Exception(
                "Invalid card file name %s. Card file names should be of form TYPE-HASH.html"
                % card_file_name
            )
        card_type, card_hash = None, None
        card_type, card_hash = file_split
        card_hash = card_hash.split(".html")[0]
        return CardInfo(card_type, card_hash)

    def save_card(self, card_type, card_html, overwrite=True):
        card_file_name = card_type
        card_path = self.get_card_location(
            self._get_card_path(), card_file_name, card_html
        )
        self._backend.save_bytes(
            [(card_path, BytesIO(bytes(card_html, "utf-8")))], overwrite=overwrite
        )

    def _list_card_paths(self, card_type=None, card_hash=None):
        card_path = self._get_card_path()
        card_paths = self._backend.list_content([card_path])
        if len(card_paths) == 0:
            # If there are no files found on the Path then raise an error of
            raise CardNotPresentException(
                self._flow_name,
                self._run_id,
                self._step_name,
                card_hash=card_hash,
                card_type=card_type,
            )
        cards_found = []
        for task_card_path in card_paths:
            card_path = task_card_path.path
            card_info = self.card_info_from_path(card_path)
            if card_type is not None and card_info.type != card_type:
                continue
            elif card_hash is not None:
                if (
                    card_info.hash != card_hash
                    and card_hash != card_info.hash[:NUM_SHORT_HASH_CHARS]
                ):
                    continue

            if task_card_path.is_file:
                cards_found.append(card_path)

        return cards_found

    def get_card_names(self, card_paths):
        return [self.card_info_from_path(path) for path in card_paths]

    def get_card_html(self, path):
        with self._backend.load_bytes([path]) as get_results:
            for _, path, _ in get_results:
                if path is not None:
                    with open(path, "r") as f:
                        return f.read()

    def cache_locally(self, path):
        with self._backend.load_bytes([path]) as get_results:
            for key, path, meta in get_results:
                if path is not None:
                    main_path = path
                    file_name = key.split("/")[-1]
                    main_path = os.path.join(self._temp_card_save_path, file_name)
                    shutil.copy(path, main_path)
                    return main_path

    def extract_card_paths(self, card_type=None, card_hash=None):
        return self._list_card_paths(
            card_type=card_type,
            card_hash=card_hash,
        )
