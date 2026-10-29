import warnings

# Root level conftest to ignore scripts folder in doctests.
warnings.filterwarnings(
    "ignore", message=".*py.path.local.*"
)


def pytest_ignore_collect(path):
    if "scripts" in str(path):
        return True
