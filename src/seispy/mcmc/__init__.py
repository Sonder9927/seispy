"""Lazy MCMC preparation and collection interfaces."""

from importlib import import_module

_EXPORTS = {
    "init_grids": ("seispy.mcmc.preparation", "init_grids"),
    "collect_results": ("seispy.mcmc.results", "collect_results"),
}


def __getattr__(name: str):
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))


__all__ = list(_EXPORTS)
