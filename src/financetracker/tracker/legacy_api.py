"""Deprecated compatibility import for the tracker runtime.

New code imports focused use cases or ``tracker.runtime`` explicitly.  This
module deliberately owns no configuration, sessions or orchestration.
"""

from financetracker.tracker import runtime as _runtime


def __getattr__(name):
    return getattr(_runtime, name)


def __dir__():
    return sorted(set(globals()) | set(dir(_runtime)))
