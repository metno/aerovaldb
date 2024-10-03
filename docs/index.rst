***************************
aerovaldb - Aeroval Dababase-interface
***************************

Website of aerovaldb, the database interface to read data of the aeroval web-api, i.e. https://api.aeroval.met.no/docs
and connect it pyaercom https://pyaerocom.readthedocs.io/


About
============

TBD

Usage Example
============

Reading
------------

.. code-block:: python
   
   import aerovaldb
   with aerovaldb.open('json_files:path/to/data/') as db:
       try:
           fh = db.get_map(*args, access_type=aerovaldb.AccessType.FILE_PATH)
           # ... sendfile of filehandle
       except FileNotFoundError as e:
           json = db.get_map(*args, access_type=aerovaldb.AccessType.JSON_STR)
           # ... send json string to client

Writing
------------

.. code-block:: python
   
   import aerovaldb
   import json
   
   with aerovaldb.open('json_files:path/to/data/') as db:
       obj = {"data": "Some test data"}
       json_str = "{ 'data': 'Some test data' }"
       db.put_map(json_str) # String is assumed to be json string and stored directly.
   
       db.put_map(obj) # Otherwise serialized object is stored.


.. toctree::
   :maxdepth: 1
   :caption: Contents:

   index
   installation
   api
   locking
   extending-aerovaldb
   genindex

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
