import os
import time
import subprocess

from threading import Thread
import tempfile
import json
from metaflow.sidecar_messages import MessageTypes, Message
from metaflow.exception import MetaflowException


class CardUpdateSidecar(object):
    def __init__(self):
        from metaflow.debug import debug
        from metaflow.plugins import CARDS

        self.periodic_cards = {c.type: c for c in CARDS if c.periodic}
        self.req_thread = Thread(target=self.periodic_updates)
        self.req_thread.daemon = True
        self.default_frequency_secs = 2
        self._debug = debug
        # debug.log("cards","creating cards")

    def process_message(self, msg):
        # type: (Message) -> None
        if msg.msg_type == MessageTypes.SHUTDOWN:
            # todo shutdown doesnt do anything yet? should it still be called
            self.shutdown()
        if (not self.req_thread.is_alive()) and msg.msg_type == MessageTypes.LOG_EVENT:

            self._card_args = [dict(**m, hash=None) for m in msg.payload]
            self.req_thread.start()

    def _render_cards(self):
        for card_meta in self._card_args:
            self._render_card(card_meta)

    def _get_periodic_render_components(self, card_type):
        component_strings = []
        if card_type in self.periodic_cards:
            component_strings = self.periodic_cards[card_type].periodic_render()
        return component_strings

    def _render_card(self, card_meta):
        temp_file = None

        component_strings = self._get_periodic_render_components(card_meta["type"])
        if len(component_strings) > 0:
            temp_file = tempfile.NamedTemporaryFile("w", suffix=".json")
            json.dump(component_strings, temp_file)
            temp_file.seek(0)

        cmd = [x for x in card_meta["create_command"]]
        if temp_file is not None:
            cmd += ["--component-file", temp_file.name]

        rep, fail = self._run_command(cmd, os.environ, card_meta["timeout"])

    def _run_command(self, cmd, env, timeout=None):
        fail = False
        timeout_args = {}
        if timeout is not None:
            timeout_args = dict(timeout=int(timeout) + 10)
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

    def periodic_updates(self):
        while True:
            try:
                self._render_cards()
                time.sleep(self.default_frequency_secs)
            except Exception as e:
                self._debug("card", str(e))
                break
                pass

    def shutdown(self):
        pass
