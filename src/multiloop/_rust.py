"""Shared helpers for lazy-importing the ``_multiloop_core`` Rust extension."""

from typing import Any

__all__ = ["_try_import_rust_class"]


def _try_import_rust_class(module_path: str, class_name: str) -> Any | None:
    """Import a class from the Rust extension, or return ``None`` if it is missing.

    The compiled ``_multiloop_core`` Rust extension is required at runtime (no
    pure-Python fallback exists). Deferred import only avoids import-time crashes
    for documentation and type inspection when the binary extension is absent.

    :param module_path: Full dotted module path (e.g. ``"multiloop._multiloop_core"``).
    :param class_name: Name of the class to import from that module.
    :returns: The class object, or ``None`` if the extension is unavailable.
    """
    try:
        mod = __import__(module_path, fromlist=[class_name])
        return getattr(mod, class_name)
    except (ImportError, AttributeError):
        return None
