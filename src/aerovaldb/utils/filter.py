# These filters are used to filter end points return a subset of a file / endpoint.
# Currently this only applies to regional_stats and heatmap, which both return a
# subset of the globstats endpoint.


def filter_regional_stats(data, variable: str, network: str, layer: str, **kwargs):
    """
    Filters regional stats out of a glob_stats data object.

    :data : Data object to operate on.
    :variable : Variable name.
    :network : Observation network.
    :layer : Layer.
    """
    return data[variable][network][layer]


def filter_heatmap(data, region: str, time: str, **kwargs):
    """
    Filters heatmap data out of a glob stats data object.

    :data : Data object to operate on.
    :region : Region ID.
    :time : Time.
    """
    filtered_data = {}  # type: ignore
    for variable, variable_data in data.items():
        filtered_data.setdefault(variable, {})
        for network, network_data in variable_data.items():
            filtered_data[variable].setdefault(network, {})
            for layer, layer_data in network_data.items():
                filtered_data[variable][network].setdefault(layer, {})
                for model, model_data in layer_data.items():
                    filtered_data[variable][network][layer].setdefault(model, {})
                    for model_var, model_var_data in model_data.items():
                        filtered_data[variable][network][layer][model].setdefault(
                            model_var, {}
                        )
                        if region in model_var_data:
                            region_data = model_var_data[region]
                            if time in region_data:
                                filtered_data[variable][network][layer][model][
                                    model_var
                                ][region] = {time: region_data[time]}

    return filtered_data


def filter_contour(data, timestep: str | None = None, **kwargs):
    if timestep is None:
        return data

    return data[timestep]
