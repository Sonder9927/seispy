"""Lazy StationXML analysis and manipulation interfaces."""

from importlib import import_module

_EXPORTS = {
    "InventoryAnalysis": ("seispy.inventory.analysis", "InventoryAnalysis"),
    "InventoryIssue": ("seispy.inventory.analysis", "InventoryIssue"),
    "InventorySuitability": ("seispy.inventory.analysis", "InventorySuitability"),
    "analyze_inventory": ("seispy.inventory.analysis", "analyze_inventory"),
    "combine_inventories": ("seispy.inventory.operations", "combine_inventories"),
    "select_inventory": ("seispy.inventory.operations", "select_inventory"),
    "shift_channel_starttime": (
        "seispy.inventory.operations",
        "shift_channel_starttime",
    ),
    "write_inventory": ("seispy.inventory.operations", "write_inventory"),
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
