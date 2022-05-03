import functools
import random
import time
from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import NGROK_KEY
from .remote_pdb import RemotePdb
from pdb import set_trace
from .remote_pdb import cry as log_message
import os


class DebugDecorator(StepDecorator):
    name = "debugger"

    defaults = dict(
        host="localhost",
        port=9983,
        auth_token=None,
    )

    def _validate_ngrok(self):
        if not self.attributes["auth_token"]:
            raise MetaflowException(
                "Ngrok `auth_token` required when calling @debugger with @batch/@kubernetes. "
                "Set the token via `auth_token` in @debugger or set the `METAFLOW_NGROK_KEY` environment variable."
            )
        return True

    def __init__(self, attributes=None, statically_defined=False):
        super().__init__(attributes, statically_defined)
        self._isvalid = True
        self.backend = None
        self._is_remote = False

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        remote_decos = [
            deco for deco in decorators if deco.name in ["kubernetes", "batch"]
        ]
        if len(remote_decos) > 0:
            self.backend = "ngrok"
            self._is_remote = True

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        self.port = self.attributes["port"]
        # print("Using Port ",self.port)
        if self.backend == "ngrok":
            if not self.attributes["auth_token"]:
                if NGROK_KEY:
                    self.attributes["auth_token"] = NGROK_KEY
            self._isvalid = self._validate_ngrok()

        from metaflow import current

        breakpoint = BreakPoint(
            self.attributes["host"],
            self.port,
            is_remote=self._is_remote,
            is_active=self._isvalid,
            auth_token=self.attributes["auth_token"],
        )
        current._update_env({"debug": breakpoint})


class BreakPoint(object):
    def __init__(self, host, port, is_remote=False, is_active=False, auth_token=None):
        self._host, self._port, self._is_remote, self._is_active, self._auth_token = (
            host,
            port,
            is_remote,
            is_active,
            auth_token,
        )

        self._ngrok_tunnel, self._debug_host, self._debug_port = None, None, None
        self._activated = False
        self._remote_pdb = None

    def _setup_ngrok_tunnel(self):
        from metaflow._vendor.pyngrok import ngrok

        if not self._auth_token:
            return None
        ngrok.set_auth_token(self._auth_token)
        debugger_terminal = ngrok.connect(self._port, "tcp")
        debug_host, debug_port = debugger_terminal.public_url.split("tcp://")[1].split(
            ":"
        )
        return debugger_terminal, debug_host, debug_port

    @property
    def breakpoint(self):
        if self._activated:
            return self._remote_pdb.set_trace()

        if not self._is_active:
            return None

        if self._is_remote:
            if not self._ngrok_tunnel:
                (
                    self._ngrok_tunnel,
                    self._debug_host,
                    self._debug_port,
                ) = self._setup_ngrok_tunnel()
            log_message(
                "Starting a Remote Debug Tunnel Using Ngrok on tcp://%s:%s. "
                "Connect to this job from your local machine using : `telnet %s %s`"
                % (
                    self._debug_host,
                    self._debug_port,
                    self._debug_host,
                    self._debug_port,
                )
            )
        else:
            log_message(
                "Connect to this job's debugger using : `telnet %s %s`"
                % (self._host, self._port)
            )
        self._activated = True
        self._remote_pdb = RemotePdb(self._host, self._port, quiet=True)
        return self._remote_pdb.set_trace()