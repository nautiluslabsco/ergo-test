import inspect
import multiprocessing
import os
import pathlib
import re
import tempfile
import time
from abc import ABC, abstractmethod
from contextlib import ContextDecorator, contextmanager
from typing import Dict, List, Optional, Type, Union

import yaml
from ergo.ergo_cli import ErgoCli

COMPONENT_TARGET = Union[str, pathlib.Path, callable]


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

    def __init__(self):
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
    def __init__(self, target: COMPONENT_TARGET, **manifest_kwargs):
        super().__init__()
        if isinstance(target, pathlib.Path):
            target = str(target)
        if isinstance(target, str):
            with open(target, "r") as fh:
                self._config = yaml.safe_load(fh)
            handler_relpath, self.handler_name = self._config["func"].rsplit(":")
            self.handler_path = resolve_handler_path(handler_relpath)
            manifest_kwargs["func"] = f"{self.handler_path}:{self.handler_name}"
        else:
            if inspect.isfunction(target):
                self.handler_path = inspect.getfile(target)
                self.handler_name = target.__name__
            else:
                # func is an instance method, and we have to get hacky to find the module variable it was assigned to
                frame = inspect.currentframe()
                frame = inspect.getouterframes(frame)[2]
                string = inspect.getframeinfo(frame[0]).code_context[0].strip()
                self.handler_path = inspect.getfile(target.__call__)
                self.handler_name = re.search(r"\((.*?)[,)]", string).group(1)
            manifest_kwargs["func"] = f"{self.handler_path}:{self.handler_name}"

            handler_module = pathlib.Path(self.handler_path).with_suffix("").name
            manifest_kwargs["subtopic"] = manifest_kwargs.get("subtopic", f"{handler_module}_{self.handler_name}_sub")
            manifest_kwargs["pubtopic"] = manifest_kwargs.get("pubtopic", f"{handler_module}_{self.handler_name}_pub")
            self._config = manifest_kwargs

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
