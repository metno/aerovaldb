from packaging.version import Version


def post_process_maps_args_kwargs(
    args: dict[str, str], kwargs: dict[str, str]
) -> tuple[dict[str, str], dict[str, str]]:
    # See this issue.
    # https://github.com/metno/aerovaldb/issues/119
    if "-" in args["obsvar"]:
        splt = args["obsvar"].split("-")

        args["obsvar"] = splt[-1]
        args["network"] = args["network"] + f"-{'-'.join(splt[:-1])}"

    if "-" in args["modvar"]:
        splt = args["modvar"].split("-")

        args["modvar"] = splt[-1]
        args["model"] = args["model"] + f"-{'-'.join(splt[:-1])}"

    return args, kwargs


def post_process_timeseries_args_kwargs(
    args: dict[str, str], kwargs: dict[str, str]
) -> tuple[dict[str, str], dict[str, str]]:
    if "-" in args["obsvar"]:
        splt = args["obsvar"].split("-")
        args["obsvar"] = splt[-1]

        args["network"] = args["network"] + f"-{'-'.join(splt[:-1])}"

    return args, kwargs


def post_process_scatter_args_kwargs(
    args: dict[str, str], kwargs: dict[str, str]
) -> tuple[dict[str, str], dict[str, str]]:
    if "-" in args["obsvar"]:
        splt = args["obsvar"].split("-")

        args["obsvar"] = splt[-1]
        args["network"] = args["network"] + f"-{'-'.join(splt[:-1])}"

    return args, kwargs


def post_process_heatmap_ts_args_kwargs(
    args: dict[str, str], kwargs: dict[str, str], *, version
) -> tuple[dict[str, str], dict[str, str]]:
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

    kwargs["layer"] = splt[-1]
    del splt[-1]

    kwargs["obsvar"] = splt[-1]
    del splt[-1]

    kwargs["region"] = splt[0]
    del splt[0]

    kwargs["network"] = "-".join(splt)

    return args, kwargs


def post_process_forecast_args_kwargs(
    args: dict[str, str], kwargs: dict[str, str]
) -> tuple[dict[str, str], dict[str, str]]:
    if "-" in args["obsvar"]:
        splt = args["obsvar"].split("-")

        args["obsvar"] = splt[-1]

        args["network"] = args["network"] + f"-{'-'.join(splt[:-1])}"

    return args, kwargs
