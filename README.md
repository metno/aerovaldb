# aerovaldb - Database combining aeroval and pyaerocom


## About

aerovaldb is the database-interface using in the aeroval-API to send data to
aeroval database instances like https://aeroval.met.no . Data is usually provided
by [PyAerocom](https://pyaerocom.readthedocs.io).

The scope of aerovaldb are:

1. A well-tested read-interface for reading resources as python-object, json or json_filehandle.
2. A interface to write/extend python objects to the resources2.
3. At least one implementation (largely based on json files) implementing the read-interface. (needed for testing)
4. At least one implementation (largely based on json files) implementing the write-interface.



## Installation
`python -m pip install 'pyaro@git+https://github.com/metno/aerovaldb.git'`


## Usage

### Reader, e.g. webserver
```python

import aerovaldb
with aerovaldb.JSON_Filehandle_DB('path/to/data/') as db:
    try:
        fh = db.get_geojson_map(type=JSON_FILEHANDLE)
        # ... sendfile of filehandle
    catch FileHandleNotAvailableException ex:
        json = db.get_geosjon_map(type=JSON)
        # ... send json string to client

```

### Writer
```
import aerovaldb
with aerovaldb.JSON_Filehandle_DB('path/to/data/') as db:
    obj = db.get_model_timeseries(type=OBJ) # type=OBJ is default
    obj["new_model"] = modeltimeseries
    db.put_model_timeseries(obj, type=OBJ) # type=OBJ is default

```




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

