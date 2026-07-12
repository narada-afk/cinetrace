"""
Auto-import every rule module in this package so @register runs.
Adding a discovery rule = dropping one module here. Nothing else.
"""

import importlib
import pkgutil

from engine.discovery.base import all_rules, get_rule, register  # noqa: F401

for _mod in pkgutil.iter_modules(__path__):
    if _mod.name not in ("base",):
        importlib.import_module(f"{__name__}.{_mod.name}")
