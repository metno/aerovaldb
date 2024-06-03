def filter_regional_stats(data, **kwargs):
    if not "variable" in kwargs:
        ValueError(f"Missing 'variable' so can't apply filters.")
    if not "network" in kwargs:
        ValueError(f"Missing 'network' so can't apply filters.")
    if not "layer" in kwargs:
        ValueError(f"Missing 'layer' so can't apply filters.")

    variable = kwargs["variable"]
    network = kwargs["network"]
    layer = kwargs["layer"]

    return data[variable][network][layer]


def filter_heatmap(data, **kwargs):
    raise NotImplementedError
