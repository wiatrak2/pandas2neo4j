from typing import Any, Dict, List, Iterable

import pandas as pd
import py2neo
from py2neo import ogm


class PandasModel(ogm.Model):
    """
    Subclass of :class:`ogm.Model` designed to ease models creation based on rows of a `pandas.DataFrame`.

    A :class:`.PandasModel` subclass should have its properties named same as the columns of the table
    that is going to be used for instances initialization. An instance can be then initialized with
    a single row of such table - it will select only columns matching the class' properties and use them
    to set the properties' values.

    :meth:`properties.SchemaProperty` subclasses can be used to specify properties along with their types.
    """
    def __init__(self, row: pd.Series):
        for column in [col for col in row.keys() if hasattr(self, col)]:
            self.__setattr__(column, row[column])

    def to_dict(self, properties: List[str] = None) -> Dict[str, Any]:
        """
        Create dictionary with properties and their values. This method is used to create `pandas.DataFrame`
        table from :class:`.PandasModel` nodes available in the graph.

        :param properties: List of properties that should be included in the dictionary. If not used
            all the available properties are returned.
        :type properties: List[str], optional
        :return: Dictionary with properties names and their values.
        """
        if properties is None:
            return dict(self.__node__.items())
        return {p: getattr(self, p) for p in properties}


def models_to_dataframe(models: Iterable[PandasModel], columns: List[str] = None) -> pd.DataFrame:
    """
    Construct a `pandas.DataFrame` from given collection of :class:`.PandasModel` instances. Each row of
    created table corresponds to a single node. If `columns` is used it determines properties stored
    in the table, otherwise the `DataFrame` contains column for each available property of :class:`.PandasModel`.

    :param models: Collection of :class:`.PandasModel` instances that should be used to construct the table.
    :type models: Iterable[:class:`.PandasModel`]
    :param columns: List of columns of constructed table. Column names must match names of :class:`.PandasModel`
        properties. If not used all the available properties are used to construct each row.
    :type columns: List[str], optional
    :return: :class:`pandas.DataFrame` table where rows correspond to properties values of `models`.
    """
    return pd.DataFrame((m.to_dict(columns) for m in models))


def nodes_to_dataframe(nodes: Iterable[py2neo.Node], columns: List[str] = None) -> pd.DataFrame:
    """
    Construct a `pandas.DataFrame` from given collection of :class:`py2neo.Node` nodes. Each row of
    created table corresponds to a single node.

    :param nodes: Collection of :class:`py2neo.Node` instances that should be used to construct the table.
    :type nodes: Iterable`[:class:`py2neo.Nodes`]
    :param columns: List of columns of constructed table. If used they must match names of nodes properties.
        If not used the table's columns are constructed based on all available properties.
    :type columns: List[str], optional
    :return: :class:`pandas.DataFrame` table where rows correspond to properties values of `nodes`.
    """
    nodes_properties = []
    for node in nodes:
        node_dict = dict(node.items())
        if columns is not None:
            node_dict = {k: node_dict[k] for k in columns}
        nodes_properties.append(node_dict)
    return pd.DataFrame(nodes_properties)
