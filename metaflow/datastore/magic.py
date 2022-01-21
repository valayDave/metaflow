import tempfile
from io import FileIO
import os
import shutil
from .local_storage import LocalStorage


def is_file_present(path):
    try:
        os.stat(path)
        return True
    except FileNotFoundError:
        return False
    except:
        raise


class MagicDirectory:

    PARENT_DIR = "magic-folder"

    def __init__(self, task_datastore):
        self._tempdir = tempfile.TemporaryDirectory(".metaflow.magic.")
        self._task_datastore = task_datastore

    def _current_files(self):
        return self._get_file_list(self._tempdir.name)

    def _get_file_list(self, directory_path, root=None):
        import os

        if root is None:
            root = directory_path
        files = []
        for dirpath, dirnames, filenames in os.walk(directory_path):
            for file in filenames:
                file_path = os.path.join(dirpath, file)
                files.append((file_path, os.path.relpath(file_path, root)))
            for _dir in dirnames:
                files.extend(
                    self._get_file_list(os.path.join(dirpath, _dir), root=root)
                )
        return files

    def __fspath__(self):
        return self._tempdir.name

    def _save(self):
        import os

        dir_files = self._current_files()
        to_store_dict = {}
        for fp, relp in dir_files:
            to_store_dict[self._make_file_name(relp)] = FileIO(str(fp), mode="r")

        self._task_datastore._save_file(to_store_dict)

    @property
    def path(self):
        return self._tempdir.name

    @property
    def root(self):
        return self._root_path(self._task_datastore, self._task_datastore._attempt)

    @classmethod
    def _make_path(cls, root, taskpath, file_name_with_attempt):
        return os.path.join(root, taskpath, file_name_with_attempt)

    @classmethod
    def _make_file_name(cls, pth):
        return os.path.join(cls.PARENT_DIR, pth)

    @property
    def files(self):
        files = []
        for _, relp in self._current_files():
            files.append(
                self._make_path(
                    self._task_datastore._storage_impl.datastore_root,
                    self._task_datastore._path,
                    self._task_datastore._metadata_name_for_attempt(
                        self._make_file_name(relp)
                    ),
                )
            )
        return files

    def sync(self):
        self._save()

    @classmethod
    def _load_files(cls, task_datastore, attempt, target_dir):
        def recursively_list_content(impl, root):
            files = []
            for lcr in impl.list_content([root]):
                if not lcr.is_file:
                    files.extend(recursively_list_content(impl, lcr.path))
                else:
                    files.append(lcr.path)
            return files

        root = cls._root_path(task_datastore, attempt)
        rel_path_to_root = os.path.relpath(
            root, task_datastore._storage_impl.datastore_root
        )
        impl = task_datastore._storage_impl
        files = recursively_list_content(impl, rel_path_to_root)
        for relfilepath in files:
            # print('relfilepath',relfilepath)
            path_relative_to_root = os.path.relpath(relfilepath, rel_path_to_root)
            # print('path_relative_to_root',path_relative_to_root)
            with impl.load_bytes([relfilepath]) as get_results:
                key, path, meta = list(get_results)[0]
                # print(key, path, meta)
                if path is not None:
                    final_folder_path = os.path.join(
                        target_dir, os.path.dirname(path_relative_to_root)
                    )
                    if not is_file_present(final_folder_path):
                        LocalStorage._makedirs(final_folder_path)
                    shutil.copy(path, os.path.join(target_dir, path_relative_to_root))

    @classmethod
    def _root_path(cls, task_datastore, attempt):
        return cls._make_path(
            task_datastore._storage_impl.datastore_root,
            task_datastore._path,
            task_datastore.metadata_name_for_attempt(cls._make_file_name(""), attempt),
        )

    def __str__(self):
        return self._tempdir.name

    def __repr__(self):
        return self._tempdir.name
