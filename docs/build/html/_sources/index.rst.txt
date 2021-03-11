.. pandas2neo4j documentation master file, created by
   sphinx-quickstart on Tue Feb 23 21:37:29 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

pandas2neo4j documentation
========================================
This package provides facilities design to ease integration between `pandas`_ tables
represented as :class:`pandas.DataFrame` objects and `neo4j`_ graph database. To interact
with the database `py2neo`_ client library is used.

:class:`.PandasGraph` is the core of `padnas` - `neo4j` connection. It allows to create
objects in remote graph based on `DataFrame` values as well as storing the graph's data in
form of the `DataFrame` tables.

The `pandas2neo4j` implements some extensions of `py2neo` classes like :class:`.PandasModel`
which represents a model class which instance can be easily obtained from a `DataFrame` row
or :class:`.SchemaProperty` which ensures that node's properties follow defined schema.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. _pandas: https://pandas.pydata.org
.. _neo4j: https://neo4j.com
.. _py2neo: https://py2neo.readthedocs.io/en/latest/index.html

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
