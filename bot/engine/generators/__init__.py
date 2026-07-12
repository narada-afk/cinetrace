"""Auto-import generator modules so @register runs."""

import importlib
import pkgutil

from engine.generators.base import all_generators, get_generator, register  # noqa: F401

for _mod in pkgutil.iter_modules(__path__):
    if _mod.name not in ("base",) and not _mod.name.startswith("_"):
        importlib.import_module(f"{__name__}.{_mod.name}")
