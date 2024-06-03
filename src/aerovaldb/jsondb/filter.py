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


def filter_entries(data, region, time):
    filtered_data = {}
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


def filter_heatmap(data, **kwargs):
    if not "region" in kwargs:
        ValueError(f"Missing 'region' so can't apply filters.")
    if not "time" in kwargs:
        ValueError(f"Missing 'time' so can't apply filters.")

    region = kwargs["region"]
    time = kwargs["time"]

    filtered_data = {}
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
