from packaging.version import Version

from ..routes import Route


# The motivation for doing this is explained here:
# https://github.com/metno/aerovaldb/issues/119
# tl;dr: it works around ambiguity in parsing of legacy
# filenames, to be able to extract metadata from file
# names reliably.
def post_process_args(
    route: Route, args: dict[str, str], kwargs: dict[str, str], *, version: Version
) -> tuple[dict[str, str], dict[str, str]]:
    if route == Route.MAP:
        args, kwargs = _post_process_maps_args_kwargs(args, kwargs)
    elif route in [Route.TIMESERIES, Route.TIMESERIES_WEEKLY]:
        args, kwargs = _post_process_timeseries_args_kwargs(
            args, kwargs, version=version
        )
    elif route == Route.HEATMAP_TIMESERIES:
        args, kwargs = _post_process_heatmap_ts_args_kwargs(
            args, kwargs, version=version
        )
    elif route == Route.FORECAST:
        args, kwargs = _post_process_forecast_args_kwargs(args, kwargs)
    elif route == Route.SCATTER:
        args, kwargs = _post_process_scatter_args_kwargs(args, kwargs)
    return args, kwargs


def _post_process_maps_args_kwargs(
    args: dict[str, str], kwargs: dict[str, str]
) -> tuple[dict[str, str], dict[str, str]]:
    if "-" in args["obsvar"]:
        splt = args["obsvar"].split("-")

        args["obsvar"] = splt[-1]
        args["network"] = args["network"] + f"-{'-'.join(splt[:-1])}"

    if "-" in args["modvar"]:
        splt = args["modvar"].split("-")

        args["modvar"] = splt[-1]
        args["model"] = args["model"] + f"-{'-'.join(splt[:-1])}"

    return args, kwargs


def _post_process_timeseries_args_kwargs(
    args: dict[str, str], kwargs: dict[str, str], *, version
) -> tuple[dict[str, str], dict[str, str]]:
    if version >= Version("0.26.0"):
        return args, kwargs

    if "-" in args["obsvar"]:
        splt = args["obsvar"].split("-")
        args["obsvar"] = splt[-1]

        args["network"] = args["network"] + f"-{'-'.join(splt[:-1])}"

    if "_" in args["network"]:
        splt = args["network"].split("_")
        args["network"] = splt[-1]

        args["location"] = args["location"] + f"_{'_'.join(splt[:-1])}"

    return args, kwargs


def _post_process_scatter_args_kwargs(
    args: dict[str, str], kwargs: dict[str, str]
) -> tuple[dict[str, str], dict[str, str]]:
    if "-" in args["obsvar"]:
        splt = args["obsvar"].split("-")

        args["obsvar"] = splt[-1]
        args["network"] = args["network"] + f"-{'-'.join(splt[:-1])}"

    return args, kwargs


def _post_process_heatmap_ts_args_kwargs(
    args: dict[str, str], kwargs: dict[str, str], *, version
) -> tuple[dict[str, str], dict[str, str]]:
    if version >= Version("0.26.0"):
        return args, kwargs
    if version <= Version("0.12.2"):
        return args, kwargs
    if version <= Version("0.13.2"):
        if "-" in kwargs["obsvar"]:
            splt: list[str] = kwargs["obsvar"].split("-")
            kwargs["obsvar"] = splt[-1]
            kwargs["network"] = kwargs["network"] + f"-{'-'.join(splt[:-1])}"

        return args, kwargs

    string = "-".join(
        [kwargs["region"], kwargs["network"], kwargs["obsvar"], kwargs["layer"]]
    )
    splt = string.split("-")

    # We know that neither layer nor obsvar can contain -.
    kwargs["layer"] = splt[-1]
    del splt[-1]
    kwargs["obsvar"] = splt[-1]
    del splt[-1]

    # Region can contain - but not _ (otherwise becomes impossible to disambiguate).
    kwargs["region"] = splt[0]
    del splt[0]

    kwargs["network"] = "-".join(splt)

    return args, kwargs


def _post_process_forecast_args_kwargs(
    args: dict[str, str], kwargs: dict[str, str]
) -> tuple[dict[str, str], dict[str, str]]:
    if "-" in args["obsvar"]:
        splt = args["obsvar"].split("-")

        args["obsvar"] = splt[-1]

        args["network"] = args["network"] + f"-{'-'.join(splt[:-1])}"

    return args, kwargs
