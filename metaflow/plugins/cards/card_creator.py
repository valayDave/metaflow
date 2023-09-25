import time
import subprocess
import tempfile
import json
import sys
import os
from metaflow import current

ASYNC_TIMEOUT = 30


def warning_message(message, logger=None, ts=False):
    msg = "[@card WARNING] %s" % message
    if logger:
        logger(msg, timestamp=ts, bad=True)


class CardProcessManager:
    """
    This class is responsible for managing the card creation processes.

    """

    async_card_processes = {
        # "carduuid": {
        #     "proc": subprocess.Popen,
        #     "started": time.time()
        # }
    }

    @classmethod
    def _register_card_process(cls, carduuid, proc):
        cls.async_card_processes[carduuid] = {
            "proc": proc,
            "started": time.time(),
        }

    @classmethod
    def _get_card_process(cls, carduuid):
        proc_dict = cls.async_card_processes.get(carduuid, None)
        if proc_dict is not None:
            return proc_dict["proc"], proc_dict["started"]
        return None, None

    @classmethod
    def _remove_card_process(cls, carduuid):
        if carduuid in cls.async_card_processes:
            cls.async_card_processes[carduuid]["proc"].kill()
            del cls.async_card_processes[carduuid]


class CardCreator:

    state_manager = None  # of type `CardStateManager`

    def __init__(
        self,
        base_command=None,
        pathspec=None,
        # TODO Add a pathspec somewhere here so that it can instantiate correctly.
    ):
        self._base_command = base_command
        self._pathspec = pathspec

    @property
    def pathspec(self):
        return self._pathspec

    def _dump_state_to_dict(self):
        return {
            "base_command": self._base_command,
            "pathspec": self._pathspec,
        }

    def create(
        self,
        card_uuid=None,
        user_set_card_id=None,
        runtime_card=False,
        decorator_attributes=None,
        card_options=None,
        logger=None,
        mode="render",
        final=False,
        component_serialzer=None,
        fetch_latest_data=None,
    ):
        # warning_message("calling proc for uuid %s" % self._card_uuid, self._logger)
        if mode != "render" and not runtime_card:
            # silently ignore runtime updates for cards that don't support them
            return
        elif mode == "refresh":
            # don't serialize components, which can be a somewhat expensive operation,
            # if we are just updating data
            component_strings = []
        else:
            component_strings = component_serialzer()
        data = fetch_latest_data(final=final)
        runspec = "/".join(self._pathspec.split("/")[1:])
        self._run_cards_subprocess(
            card_uuid,
            user_set_card_id,
            mode,
            runspec,
            decorator_attributes,
            card_options,
            component_strings,
            logger,
            data,
        )

    def _run_cards_subprocess(
        self,
        card_uuid,
        user_set_card_id,
        mode,
        runspec,
        decorator_attributes,
        card_options,
        component_strings,
        logger,
        data=None,
    ):
        components_file = data_file = None
        wait = mode == "render"

        if len(component_strings) > 0:
            # note that we can't delete temporary files here when calling the subprocess
            # async due to a race condition. The subprocess must delete them
            components_file = tempfile.NamedTemporaryFile(
                "w", suffix=".json", delete=False
            )
            json.dump(component_strings, components_file)
            components_file.seek(0)
        if data is not None:
            data_file = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
            json.dump(data, data_file)
            data_file.seek(0)

        if self.state_manager is not None:
            self.state_manager._save_card_state(
                card_uuid, components=component_strings, data=data
            )

        cmd = []
        cmd += self._base_command + [
            "card",
            "create",
            runspec,
            "--delete-input-files",
            "--card-uuid",
            card_uuid,
            "--mode",
            mode,
            "--type",
            decorator_attributes["type"],
            # Add the options relating to card arguments.
            # todo : add scope as a CLI arg for the create method.
        ]
        if card_options is not None and len(card_options) > 0:
            cmd += ["--options", json.dumps(card_options)]
        # set the id argument.

        if decorator_attributes["timeout"] is not None:
            cmd += ["--timeout", str(decorator_attributes["timeout"])]

        if user_set_card_id is not None:
            cmd += ["--id", str(user_set_card_id)]

        if decorator_attributes["save_errors"]:
            cmd += ["--render-error-card"]

        if components_file is not None:
            cmd += ["--component-file", components_file.name]

        if data_file is not None:
            cmd += ["--data-file", data_file.name]

        response, fail = self._run_command(
            cmd,
            card_uuid,
            os.environ,
            timeout=decorator_attributes["timeout"],
            wait=wait,
        )
        # warning_message(str(cmd), logger)
        if fail:
            resp = "" if response is None else response.decode("utf-8")
            logger(
                "Card render failed with error : \n\n %s" % resp,
                timestamp=False,
                bad=True,
            )

    def _run_command(self, cmd, card_uuid, env, wait=True, timeout=None):
        fail = False
        timeout_args = {}
        async_timeout = ASYNC_TIMEOUT
        if timeout is not None:
            async_timeout = int(timeout) + 10
            timeout_args = dict(timeout=int(timeout) + 10)

        if wait:
            try:
                rep = subprocess.check_output(
                    cmd, env=env, stderr=subprocess.STDOUT, **timeout_args
                )
            except subprocess.CalledProcessError as e:
                rep = e.output
                fail = True
            except subprocess.TimeoutExpired as e:
                rep = e.output
                fail = True
            return rep, fail
        else:
            _async_proc, _async_started = CardProcessManager._get_card_process(
                card_uuid
            )
            if _async_proc and _async_proc.poll() is None:
                if time.time() - _async_started > async_timeout:
                    CardProcessManager._remove_card_process(card_uuid)
                else:
                    # silently refuse to run an async process if a previous one is still running
                    # and timeout hasn't been reached
                    return "".encode(), False
            else:
                CardProcessManager._register_card_process(
                    card_uuid,
                    subprocess.Popen(
                        cmd,
                        env=env,
                        stderr=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                    ),
                )
                return "".encode(), False
