Extending AerovalDB
===================

AerovalDB is designed to allow custom implementations to allow storage in new storage formats. This is done by writing a new class inheriting from :class:`aerovaldb.AerovalDB`.

Unless overridden, getters and setters will be routed to :meth:`aerovaldb.AerovalDB._get` and :meth:`aerovaldb.AerovalDB._put` respectively, so this is were the main read and write behaviour should be implemented. These functions receive a route, as well as arguments based on which to get/put data and are intended to be used based on route-lookup (for example, :class:`aerovaldb.jsondb.jsonfiledb.AerovalJsonFileDB` translates the route into a file path template, while :class:`aerovaldb.sqlitedb.sqlitedb.AerovalSqliteDB` translates the route into a table name).

URI Scheme
----------
