import warnings

# Root level conftest to ignore scripts folder in doctests.
collect_ignore = ["scripts"]

warnings.filterwarnings(
    "ignore", message=".*py.path.local.*"
)
