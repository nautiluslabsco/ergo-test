import inspect
import multiprocessing
import pathlib
import re
import os
import sys
import tempfile
import time
from abc import ABC, abstractmethod
from contextlib import ContextDecorator, contextmanager
from typing import Callable, Dict, List, Optional, Type
import importlib
import yaml

from ergo.ergo_cli import ErgoCli


class ComponentInstance:
    def __init__(self, ergo_command: str, *configs: Dict):
        self.process = multiprocessing.Process(target=ergo_subprocess_target, args=(ergo_command, configs))

    def __enter__(self):
        self.process.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.process.terminate()
        self.process.join()


def ergo_subprocess_target(ergo_command, configs):
    config_files = []
    for config in configs:
        config_file = tempfile.NamedTemporaryFile(mode="w")
        config_file.write(yaml.dump(config))
        config_file.seek(0)
        config_files.append(config_file)
    args = tuple(cf.name for cf in config_files)
    try:
        getattr(ErgoCli(), ergo_command)(*args)
    finally:
        for config_file in config_files:
            config_file.close()


def retries(n: int, backoff_seconds: float, *retry_errors: Type[Exception]):
    success: set = set()
    for attempt in range(n):
        if success:
            break

        @contextmanager
        def retry():
            try:
                yield
                success.add(True)
            except retry_errors or Exception:
                if attempt + 1 == n:
                    raise
                time.sleep(backoff_seconds)

        yield retry


class Component(ContextDecorator, ABC):
    _ergo_command = "start"

    def __init__(self, config_path: str):
        with open(config_path, "r") as fh:
            self._config = yaml.safe_load(fh)

        self._instance: Optional[ComponentInstance] = None

    @property
    @abstractmethod
    def namespace(self) -> dict:
        raise NotImplementedError

    @property
    def configs(self) -> List[dict]:
        return [self.namespace]

    def __enter__(self):
        self._instance = ComponentInstance(self._ergo_command, *self.configs)
        self._instance.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._instance.__exit__(exc_type, exc_val, exc_tb)


class FunctionComponent(Component, ABC):
    def __init__(self, config_path: str, **manifest_kwargs):
        super().__init__(config_path)
        handler_relpath, self.handler_name = self._config["func"].rsplit(":")
        self.handler_path = resolve_handler_path(handler_relpath)
        manifest_kwargs["func"] = f"{self.handler_path}:{self.handler_name}"
        self._manifest_kwargs = manifest_kwargs

    @property
    def manifest(self) -> dict:
        manifest = {k: v for k, v in self._config.items()}
        manifest.update(self._manifest_kwargs)
        return manifest

    @property
    def configs(self) -> List[dict]:
        return [self.manifest, self.namespace]


def resolve_handler_path(relpath: str) -> str:
    for search_path in pathlib.Path(os.getcwd()).parents:
        for candidate in search_path.rglob(relpath):
            return str(candidate)
    raise RuntimeError(f"failed to resolve abspath for func {relpath}")
