"""Total Perspective Vortex library setup."""

from importlib.metadata import PackageNotFoundError, version


def get_version() -> str:
    """
    Return a string with the current version of the library.

    :rtype: ``string``
    :return:  Library version (e.g., "1.0.0").
    """
    try:
        return version("total-perspective-vortex")
    except PackageNotFoundError:
        return "unknown"
