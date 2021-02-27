from typing import Any, Callable, Iterable, List, Tuple, Union

import pandas as pd
import py2neo
from py2neo import matching
from py2neo import ogm
import numpy as np

import pandas2neo4j
from pandas2neo4j.pandas_model import PandasModel
from pandas2neo4j.errors import (
    NodeWithIdDoesNotExistsError,
    NotSupportedModelClassError,
)


class PandasGraph(ogm.Repository):
    @property
    def schema(self) -> py2neo.Schema:
        return self.graph.schema

    @property
    def _node_matcher(self) -> matching.NodeMatcher:
        return matching.NodeMatcher(self.graph)

    def create_graph_object(self, subgraph: Union[ogm.Model, py2neo.Entity]):
        """
        Push object to remote graph

        :param subgraph: either :class:`py2neo.ogm.Model` of :class:`py2neo.Entity` instance.
        """
        self.graph.create(subgraph)

    def create_graph_objects(self, objects: Iterable[Union[ogm.Model, py2neo.Entity]]):
        """
        Push collection of objects to remote graph

        :param objects: an iterable of either :class:`py2neo.ogm.Model` or :class:`py2neo.Entity` instances.
        """
        tx = self.graph.begin()
        for obj in objects:
            tx.create(obj)
        tx.commit()

    def _get_node_from_model(
        self,
        model_class: ogm.Model,
        id_key: str,
        id_value: Any,
    ) -> py2neo.Node:
        del id_key
        model_instance = self.get(model_class, id_value)
        if model_instance is None:
            raise NodeWithIdDoesNotExistsError(model_class, id_value)
        return model_instance.__node__

    def _get_node_from_str(
        self,
        model_class: str,
        id_key: str,
        id_value: Any,
    ) -> py2neo.Node:
        return self.graph.nodes.match(model_class, **{id_key: id_value}).first()

    def _node_getter(self, model_class: Union[ogm.Model, str]) -> Callable:
        if type(model_class) is str:
            return self._get_node_from_str
        elif issubclass(model_class, ogm.Model):
            return self._get_node_from_model
        raise NotSupportedModelClassError

    def _get_relationship_nodes(
        self,
        row: pd.Series,
        from_model_class: Union[ogm.Model, str],
        to_model_class: Union[ogm.Model, str],
        from_key_column: str,
        to_key_column: str,
        from_model_id_key: str = None,
        to_model_id_key: str = None,
    ) -> Tuple[py2neo.Node, py2neo.Node]:
        from_node_getter = self._node_getter(from_model_class)
        to_node_getter = self._node_getter(to_model_class)
        return (
            from_node_getter(from_model_class, from_model_id_key, row[from_key_column]),
            to_node_getter(to_model_class, to_model_id_key, row[to_key_column]),
        )

    def _create_relationship(
        self,
        relationship: str,
        from_node: py2neo.Node,
        to_node: py2neo.Node,
    ) -> py2neo.Relationship:
        if from_node is None or to_node is None:
            raise NodeWithIdDoesNotExistsError
        return py2neo.Relationship(from_node, relationship, to_node)

    def create_relationships_from_dataframe(
        self,
        df: pd.DataFrame,
        relationship: str,
        from_model_class: Union[ogm.Model, str],
        to_model_class: Union[ogm.Model, str],
        from_key_column: str,
        to_key_column: str,
        from_model_id_key: str = None,
        to_model_id_key: str = None,
        chunk_size: int = 0,
    ) -> pd.Series:
        """
        Create relationships of type `relationship` between instances of `from_model_class` and `to_model_class`.
        Return a :class:`pandas.Series` of :class:`py2neo.Relationship` objects that represent each relationship
        in the table.

        Relationships are listed in `df: pandas.DataFrame` where rows contain pairs of ids sufficient to identify
        the entities that should be connected. `from_key_column` and `to_key_column` arguments specify names of the
        columns that contain these ids. By default :class:`ogm.Model`'s *__primarykey__* is used to identify the
        nodes with id values, however other properties can be used by specifying `from_model_id_key`/`to_model_id_key`.
        These values are also required if `from_model_class` or `to_model_class` are not provided as a subclass of
        `ogm.Model` but a string with node label is provided instead.

        `chunk_size` parameter can be used if the `df` table is large. It specifies the maximal number of relationships
        than can be created within a single transaction. Note that `numpy.array_split` function is used to split the
        table into chunks, so the size of each part may be different than the number specified in the `chunk_size`.

        :param df: A table with relationships key pairs. Each row should contain ids of already existing nodes in
            `from_key_column` and `to_key_column` columns.
        :type df: :class:`pandas.DataFrame`
        :param relationship: Name of the relationship that should be created
        :type relationship: str
        :param from_model_class: Either :class:`ogm.Model` subclass (e.g. subclass of :class:`pandas2neo4j.PandasModel`)
            or `str` with class name/label of :class:`py2neo.ogm.Model`/:class:`py2neo.Node` instances that
            should be starting nodes of each relationship.
        :type from_model_class: Union[:class:`ogm.Model`, str]
        :param to_model_class: Either :class:`ogm.Model` subclass (e.g. subclass of :class:`pandas2neo4j.PandasModel`)
            or `str` with class name/label of :class:`py2neo.ogm.Model`/:class:`py2neo.Node` instances that
            should be ending nodes of each relationship.
        :type to_model_class: Union[:class:`ogm.Model`, str]
        :param from_key_column: Name of the column in `df` table containing ids of the relationships starting nodes.
        :type from_key_column: str
        :param to_key_column: Name of the column in `df` table containing ids of the relationships ending nodes.
        :type to_key_column: str
        :param from_model_id_key: Name of the property that should be used to identify the node by value specified in
            `from_key_column`. If `from_model_class` is a :class:`ogm.Model` subclass the *__primarykey__* is used
            by default and this parameter can be omitted.
        :type from_model_id_key: str, optional
        :param to_model_id_key: Name of the property that should be used to identify the node by value specified in
            `to_key_column`. If `to_model_class` is a :class:`ogm.Model` subclass the *__primarykey__* is used
            by default and this parameter can be omitted.
        :type to_model_id_key: str, optional
        :param chunk_size: Maximal number of rows that should be converted into relationships within a single transation.
        :type chunk_size: int, optional
        :return: A :class:`pandas.Series` with :class:`py2neo.Relationship` objects for each row in the `df` table.
        """
        chunk_num = 1 if chunk_size == 0 else np.ceil(len(df) / chunk_size)
        all_relationships = []
        for chunk in np.array_split(df, chunk_num):
            relationships = chunk.apply(
                lambda row: self._create_relationship(
                    relationship,
                    *self._get_relationship_nodes(
                        row,
                        from_model_class,
                        to_model_class,
                        from_key_column,
                        to_key_column,
                        from_model_id_key=from_model_id_key,
                        to_model_id_key=to_model_id_key,
                    ),
                ),
                axis=1,
            )
            self.create_graph_objects(relationships)
            all_relationships.append(relationships)
        return pd.concat(all_relationships)

    def create_nodes_from_dataframe(
        self,
        df: pd.DataFrame,
        model_class: Union[ogm.Model, str],
        chunk_size: int = 0,
    ) -> pd.Series:
        """
        Create graph nodes defined in `df` table. Each row should contain data of a single node.
        `model_class` parameter determines the class/label of :class:`py2neo.ogm.Model`/:class:`py2neo.Node`
        that will be created.

        Return a :class:`pd.Series` with either :class:`py2neo.ogm.Model` subclasses or :class:`py2neo.Node` instances
        depending on `model_class` parameter. If `model_class` is a `str` the function will create a
        :class:`py2neo.Node` instance for each row, where `model_class` value will be used as each node's label
        and all the columns in the `df` will be transformed into properties. If `model_class` is an
        :class:`py2neo.ogm.Model` instance but not the :class:`pandas2neo4j.PandasModel` it must provide
        :meth:`from_pandas_series` classmethod that construct a class instance given a :class:`pandas.Series`
        containing a table's row data. If `model_class` is a subclass of :class:`pandas2neo4j.PandasModel`
        the :meth:`__init__` will be triggered.

        `chunk_size` parameter can be used if the `df` table is large and should be splitted into chunks when
        creating the nodes. It specifies the maximal numbers of graph nodes to be created within a single transaction.
        `numpy.array_split` function is used for splitting, so the size of each part may be different than
        the number passed as the parameter.

        :param df: A table containing data of nodes that should be created.
        :type df: :class:`pandas.DataFrame`
        :param model_class: either :class:`py2neo.ogm.Model` subclass or `str` determining the class/label that should
            be used to construct each object.
        :type model_class: Union[:class:`ogm.Model`, str]
        :param chunk_size: Maximal number of rows that should be converted into nodes within a single transation.
        :type chunk_size: int, optional
        :returns: A :class:`pandas.Series` with node objects of class determined by `model_class` param and properties
            provided in the `df` table.
        """
        chunk_num = 1 if chunk_size == 0 else np.ceil(len(df) / chunk_size)
        all_nodes = []
        for chunk in np.array_split(df, chunk_num):
            if isinstance(model_class, str):
                nodes = chunk.apply(lambda row: py2neo.Node(model_class, **row), axis=1)
            elif issubclass(model_class, PandasModel):
                nodes = chunk.apply(model_class, axis=1)
            elif hasattr(model_class, "from_pandas_series"):
                nodes = chunk.apply(model_class.from_pandas_series, axis=1)
            else:
                raise NotSupportedModelClassError
            self.create_graph_objects(nodes)
            all_nodes.append(nodes)
        return pd.concat(all_nodes)

    def get_graph_models(self, model_class: ogm.Model) -> List[ogm.Model]:
        return list(model_class.match(self))

    def get_graph_nodes(self, label: str) -> List[py2neo.Node]:
        return list(self._node_matcher.match(label))

    def get_nodes_for_dataframe(
        self, node_label: str, node_id_property: str, df: pd.DataFrame, id_column_name: str
    ) -> List[py2neo.Node]:
        match_condition = {node_id_property: matching.IN(df[id_column_name])}
        return list(self._node_matcher.match(node_label, **match_condition).all())

    def get_nodes_models_for_dataframe(
        self,
        model_class: ogm.Model,
        node_label: str,
        df: pd.DataFrame,
        id_column_name: str,
        node_id_property: str = None,
    ) -> pd.DataFrame:
        if node_id_property is None:
            node_id_property = model_class.__primarykey__
        models_column = pd.Series(
            self.get_nodes_for_dataframe(node_label, node_id_property, df, id_column_name)
        ).apply(model_class.wrap)
        models_dict = {
            node_id_property: models_column.apply(lambda n: getattr(n, node_id_property)),
            model_class.__name__: models_column,
        }
        return pd.DataFrame(models_dict)

    def get_dataframe_for_models(
        self, model_class: ogm.Model, columns: List[str] = None
    ) -> pd.DataFrame:
        if not hasattr(model_class, "to_dict"):
            raise NotSupportedModelClassError(
                f"Unable to construct pd.DataFrame from {model_class.__name__} model class - `to_dict` method is missing."
            )
        return pandas2neo4j.models_to_dataframe(model_class.match(self), columns)

    def get_dataframe_for_label(self, label: str, columns: List[str] = None):
        return pandas2neo4j.nodes_to_dataframe(self._node_matcher.match(label), columns)
