# aerovaldb - Database combining aeroval and pyaerocom


## About

aerovaldb is the database-interface used in the aeroval-API to send data to
aeroval database instances like https://aeroval.met.no . Data is usually provided
by [PyAerocom](https://pyaerocom.readthedocs.io).

The scope of aerovaldb are:

1. A well-tested read-interface for reading resources as python-object, json or json_filehandle.
2. A interface to write/extend python objects to the resources2.
3. At least one implementation (largely based on json files) implementing the read-interface. (needed for testing)
4. At least one implementation (largely based on json files) implementing the write-interface.

Our documentation can be found [here](https://aerovaldb.readthedocs.io/)

## Design principles

1. As pyaerocom-programmer using aerovaldb I want to have documented functions with arguments, e.g. `put_heatmap(hm, country, component)`
2. As a pyaerocom-progammer, I want to be able to write to a new datasource/format by just updating a config-file
3. As a pyaerocom-user, I want to be able to upgrade an experiment without thinking about the dabase format.
4. From a DB-implementer I want to have a resource-name (table/path) with parameters as dictionary, e.g.
    `open(f"path/to/heatmaps/{country}/{component}.json")` or `SELECT * FROM PATH_TO_HEATMAPS where country = ? and component = ?`
5. As Web-developer using aerovaldb, I have to map routes to function, and I have parameters for the routes.
6. As a Web-developer, I don't want to need to know the type of the aerovaldb-implementation and can auto-guess from a location.
7. As a web-developer, I might need to handle databases in different formats depending on when the user set up the experiment.
8. For performance, I want to avoid unnecessary json/obj transformations, so I want to put/get obj or json or filehandle
   and the database might want to decide how to handle that fastest. obj should be the default, json is required,
   filehandle is optional (and only for get-operations.).  (this requirement might be premature optimization? to be discussed ...)
9. To be discussed: do we want to allow the programmer to set open-modes, e.g. 'r', 'rw', 'w'?

## Installation
`python -m pip install 'aerovaldb@git+https://github.com/metno/aerovaldb.git'`


## Usage

### Reader, e.g. webserver

```python
import aerovaldb

with aerovaldb.open('json_files:path/to/data/') as db:
    try:
        fh = db.get_map(*args, access_type=aerovaldb.AccessType.FILE_PATH)
        # ... sendfile of filehandle
    except FileNotFoundError as e:
        json = db.get_map(*args, access_type=aerovaldb.AccessType.JSON_STR)
        # ... send json string to client

```

### Writer

```python
import aerovaldb
import json

with aerovaldb.open('json_files:path/to/data/') as db:
    db.put
    obj = {"data": "Some test data"}
    json_str = "{ 'data': 'Some test data' }"
    db.put_map(json_str) # String is assumed to be json string and stored directly.

    db.put_map(obj) # Otherwise serialized object is stored.
```

### API (version 0)

| Data               | Getter                      | Setter                      |
|--------------------|-----------------------------|-----------------------------|
| glob_stats         | db.get_glob_stats()         | db.put_glob_stats()         |
| experiments        | db.get_experiments()        | db.get_experiments()        |
| config             | db.get_config()             | db.get_config()             |
| menu               | db.get_menu()               | db.put_menu()               |
| statistics         | db.get_statistics()         | db.put_statistics()         |
| ranges             | db.get_ranges()             | db.put_ranges()             |
| regions            | db.get_regions()            | db.put_regions()            |
| models_style       | db.get_models_style()       | db.put_models_style()       |
| map                | db.get_map()                | db.put_map()                |
| time series        | db.get_timeseries()         | db.put_timeseries()         |
| time series weekly | db.get_timeseries_weekly()  | db.put_timeseries_weekly()  |
| scatter            | db.get_scatter()            | db.put_scatter()            |
| profiles           | db.get_profiles()           | db.put_profiles()           |
| heatmap timeseries | db.get_heatmap_timeseries() | db.put_heatmap_timeseries() |
| forecast           | db.get_forecast()           | db.put_forecast()           |
| contour            | db.get_contour()            | db.put_contour()            |
| gridded map        | db.get_gridded_map()        | db.put_gridded_map()        |
| report             | db.get_report()             | db.put_report()             |






## COPYRIGHT

Copyright (C) 2024  Augustin Mortier, Thorbjørn Lunding, Heiko Klein, Norwegian Meteorological Institute

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public
License as published by the Free Software Foundation; either
version 3 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU General Public
License along with this library; if not, see https://www.gnu.org/licenses/

