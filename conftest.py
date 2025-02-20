import warnings

from pytest import PytestRemovedIn9Warning

# Root level conftest to ignore scripts folder in doctests.
warnings.filterwarnings(
    "ignore", category=PytestRemovedIn9Warning, message=".*py.path.local.*"
)


def pytest_ignore_collect(path):
    if "scripts" in str(path):
        return True
