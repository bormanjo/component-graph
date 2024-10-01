import logging

import pytest

from cg.utils import import_object


def test_import_object() -> None:
    assert import_object("logging.INFO") == logging.INFO

    with pytest.raises(ImportError, match="Could not locate: *"):
        import_object("this.path.definitely.does.not.exist")
