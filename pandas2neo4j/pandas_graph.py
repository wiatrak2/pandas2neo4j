from typing import Any, Callable, Iterable, List, Tuple, Union

from cached_property import cached_property
import pandas as pd
import py2neo
from py2neo import matching
from py2neo import ogm
import numpy as np

import pandas2neo4j
from pandas2neo4j.pandas_model import PandasModel
from pandas2neo4j.errors import (
    NodeWithIdDoesNotExistError,
    NotSupportedModelClassError,
    InvalidArgumentsConfigurationError,
    RelationshipDoesNotExistError,
)


class PandasGraph(ogm.Repository):
    """
    Class representing the underlying graph.

    :class:`.PandasGraph` provides multiple facilities to operate on graph's nodes (mostly wrapped
    by :class:`ogm.Model` and :class:`.PandasModel`) using some data stored in `pandas.DataFrame`
    tables. One can create :class:`.PandasModel` instances and relationships between nodes based
    on rows of given table.
    """
    @property
    def schema(self) -> py2neo.Schema:
        """
        :class:`py2neo.Schema` object of underlying graph.
        """
        return self.graph.schema

    @cached_property
    def _node_matcher(self) -> matching.NodeMatcher:
        return matching.NodeMatcher(self.graph)

    @cached_property
    def _relationship_matcher(self) -> matching.RelationshipMatcher:
        return matching.RelationshipMatcher(self.graph)

    def create_graph_object(self, subgraph: Union[ogm.Model, py2neo.Entity]):
        """
        Push object to remote graph

        :param subgraph: either :class:`py2neo.ogm.Model` of :class:`py2neo.Entity` instance.
        """
        if hasattr(subgraph, "__node__"):
            subgraph = subgraph.__node__
        self.graph.create(subgraph)

    def create_graph_objects(self, objects: Iterable[Union[ogm.Model, py2neo.Entity]]):
        """
        Push collection of objects to remote graph

        :param objects: an iterable of either :class:`py2neo.ogm.Model` or :class:`py2neo.Entity` instances.
        """
        tx = self.graph.begin()
        for obj in objects:
            if hasattr(obj, "__node__"):
                obj = obj.__node__
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
            raise NodeWithIdDoesNotExistError(model_class, id_value)
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
            raise NodeWithIdDoesNotExistError()
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
        :param from_model_class: Either :class:`ogm.Model` subclass (e.g. subclass of :class:`.PandasModel`)
            or `str` with class name/label of :class:`py2neo.ogm.Model`/:class:`py2neo.Node` instances that
            should be starting nodes of each relationship.
        :type from_model_class: Union[:class:`ogm.Model`, str]
        :param to_model_class: Either :class:`ogm.Model` subclass (e.g. subclass of :class:`.PandasModel`)
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
        :class:`py2neo.ogm.Model` instance but not the :class:`.PandasModel` it must provide
        :meth:`from_pandas_series` classmethod that construct a class instance given a :class:`pandas.Series`
        containing a table's row data. If `model_class` is a subclass of :class:`.PandasModel`
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
        :return: A :class:`pandas.Series` with node objects of class determined by `model_class` param and properties
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
        """
        Return list with all `model_class` objects available in the graph.

        :param model_class: :class:`ogm.Model` class which objects should be returned.
        :type model_class: :class:`ogm.Model`
        """
        return list(model_class.match(self))

    def get_graph_nodes(self, label: str) -> List[py2neo.Node]:
        """
        Return list with all `py2neo.Node` nodes matching given label availbale in the graph.

        :param label: label determining nodes to return.
        :type label: str
        """
        return list(self._node_matcher.match(label))

    def get_nodes_for_dataframe(
        self, df: pd.DataFrame, node_label: str, node_id_property: str, id_column_name: str
    ) -> List[py2neo.Node]:
        """
        Match nodes in the graph with rows of `df` DataFrame. `node_label` is the label of nodes
        that should be used for the matching. From all of these nodes select ones with `node_id_property`
        property value available in the `id_column_name` column of the `df` DataFrame. Number of returned
        nodes may differ the length of `df` table.

        :param df: a table which rows describe nodes that should be found in the graph.
        :type df: :class:`pandas.DataFrame`
        :param node_label: label of nodes that should be mapped to rows of `df` table.
        :type node_label: str
        :param node_id_property: name of property that should be use to determine whether a particular
            node maps to a row of `df` table.
        :type node_id_property: str
        :param id_column_name: name of `df` table's column which values should be matched with
            `node_id_property` property of nodes.
        :type id_column_name: str
        :return: List with all :class:`py2neo.Node` objects matching the rows of `df` table.
        """
        match_condition = {node_id_property: matching.IN(df[id_column_name])}
        return list(self._node_matcher.match(node_label, **match_condition).all())

    def get_nodes_models_for_dataframe(
        self,
        df: pd.DataFrame,
        model_class: ogm.Model,
        node_label: str,
        id_column_name: str,
        node_id_property: str = None,
    ) -> pd.DataFrame:
        """
        Get all available nodes in the graph with `node_label` label matching rows of `df` table.
        Cast the matching rows to `model_class` objects. `node_id_property` determines name of
        the property that is used to map nodes in the graph to rows of the table - if the property
        matches one of values in `id_column_name` column it's going to be included in produced DataFrame.
        If `node_id_property` is not provided look for `id_column_name` property.

        This function is especially useful if one has created some nodes in the graph based on `df` rows
        (e.g. with :meth:`PandasGraph.create_nodes_from_dataframe` method) and wants to
        retreive the `model_class` objects in another execution.

        :param df: a table which rows describe nodes that should be found in the graph.
        :type df: :class:`pandas.DataFrame`
        :param model_class: the :class:`ogm.Model` class that should be used to wrap the matching nodes.
        :type model_class: :class:`ogm.Model`
        :param node_label: label of nodes that should be mapped to rows of `df` table.
        :type node_label: str
        :param id_column_name: name of `df` table's column which values should be matched with
            `node_id_property` property of nodes.
        :type id_column_name: str
        :param node_id_property: name of property that should be use to determine whether a particular
            node maps to a row of `df` table. If not provided map by property named with `id_column_name`.
        :type node_id_property: str, optional
        :return: :class:`pandas.DataFrame` table which rows contain :class:`ogm.Model` objects as well as
            the value of property used to map these nodes.
        """
        if node_id_property is None:
            node_id_property = id_column_name
        models_column = pd.Series(
            self.get_nodes_for_dataframe(df, node_label, node_id_property, id_column_name)
        ).apply(model_class.wrap)
        models_dict = {
            node_id_property: models_column.apply(lambda n: getattr(n, node_id_property)),
            model_class.__name__: models_column,
        }
        return pd.DataFrame(models_dict)

    def _match_model(
        self, model_class: Union[ogm.Model, str], **match_condition
    ) -> Union[ogm.Model, py2neo.Node]:
        if isinstance(model_class, str):
            return self._node_matcher.match(model_class).first()
        return model_class.match(self).where(**match_condition).first()

    def get_models_for_dataframe(
        self,
        df: pd.DataFrame,
        model_class: ogm.Model,
        id_column_name: str,
        node_id_property: str = None,
    ) -> pd.DataFrame:
        """
        Get all available `model_class` nodes matching rows of `df` table. For each row of the table
        return a single `model_class` object or None if a row could not be mapped to one of the graph's
        nodes.

        :param df: a table which rows describe nodes that should be found in the graph.
        :type df: :class:`pandas.DataFrame`
        :param model_class: the :class:`ogm.Model` which models should be matched and returned.
        :type model_class: :class:`ogm.Model`
        :param id_column_name: name of `df` table's column which values should be matched with
            `node_id_property` property of nodes.
        :type id_column_name: str
        :param node_id_property: name of property that should be use to determine whether a particular
            node maps to a row of `df` table. If not provided map by property named with `id_column_name`.
        :type node_id_property: str, optional
        :return: :class:`pandas.DataFrame` table which one column is a duplicate of df[id_column_name] and
            the other contains corresponding `model_class` objects.
        """
        if node_id_property is None:
            node_id_property = id_column_name
        models_df = df[[id_column_name]]
        models_df[model_class.__name__] = df[id_column_name].apply(
            lambda id_value: self._match_model(model_class, **{node_id_property: id_value})
        )
        return models_df

    def get_dataframe_for_models(
        self, model_class: ogm.Model, columns: List[str] = None
    ) -> pd.DataFrame:
        """
        Dump `model_class` nodes available in the graph to `pandas.DataFrame`. The `model_class` must
        provide `to_dict` method that is used to construct a row for each node. If only subset of the
        dictionary items returned by the `to_dict` methods should be used one can specify them (and their
        order) with `columns` parameter.

        `to_dict` method is provided by each :class:`.PandasModel` instance.

        :param model_class: class of nodes that should be used to construct the table.
        :type model_class: :class:`ogm.Model`
        :param columns: list of produced table columns names.
        :type columns: List[str], optional.
        :return: :class:`pandas.DataFrame` which rows represent the `model_class` nodes in the graph.
        """
        if not hasattr(model_class, "to_dict"):
            raise NotSupportedModelClassError(
                f"Unable to construct pd.DataFrame from {model_class.__name__} model class - `to_dict` method is missing."
            )
        return pandas2neo4j.models_to_dataframe(model_class.match(self), columns)

    def get_dataframe_for_label(self, label: str, columns: List[str] = None):
        """
        Dump all nodes with `label` label available in the graph to `pandas.DataFrame` table. If only subset
        of nodes' properties should be used to construct each row of the table one can specify them with `columns`
        parameter.

        :param label: label of nodes that should be dumped to the table.
        :type label: str
        :param columns: list of produced table columns names.
        :type columns: List[str], optional
        :return: :class:`pandas.DataFrame` which rows represent the graph's nodes.
        """
        return pandas2neo4j.nodes_to_dataframe(self._node_matcher.match(label), columns)

    def get_relationships(
        self,
        relationship: str,
        nodes: Iterable[Union[ogm.Model, py2neo.Node]] = None,
        inner_only=False,
    ) -> List[py2neo.Relationship]:
        """
        Return list of :class:`py2neo.Relationship` objects representing given relationship available in the graph.
        If `nodes` argument is used return only relationships where one of the nodes is provided in the parameter.
        If `nodes` is used and `inner_only` is True return only relationships where both nodes are provided in `nodes`.

        :param relationship: name of the relationship which objects should be returned.
        :type relationship: str
        :parm nodes: Iterable of either :class:`ogm.Model` or :class:`py2neo.Node` nodes that should be start/end
            node of returned :class:`py2neo.Relationship` objects. If `inner_only` is True both start and end nodes
            of a relationship must be provided in `nodes` to include such relationship in the result.
        :type nodes: Iterable[Union[:class:`ogm.Model`, :class:`py2neo.Node`]]
        :param inner_only: Boolean value determining whether both start and end nodes of a single :class:`py2neo.Relationship`
            object should be available in `nodes`.
        :return: List of :class:`py2neo.Relationship` objects matching the relationship.
        """
        if nodes is None and inner_only:
            raise InvalidArgumentsConfigurationError(
                "`inner_only` argument can be used only when `nodes` are provided."
            )

        if nodes is None:
            return self._relationship_matcher.match(r_type=relationship)
        relationships = set()
        try:
            nodes_set = {node if type(node) is py2neo.Node else node.__node__ for node in nodes}
        except AttributeError:
            raise NotSupportedModelClassError(
                f"Unable to obtain `py2neo.Node` instance from provided nodes."
            )
        for node in nodes_set:
            node_relationships = set(
                self._relationship_matcher.match(nodes={node}, r_type=relationship)
            )
            if inner_only:
                node_relationships = {
                    rel for rel in node_relationships if set(rel.nodes).issubset(nodes_set)
                }
            relationships |= set(node_relationships)
        return list(relationships)

    def get_dataframe_for_relationship(
        self,
        relationship: str,
        from_node_property: str,
        to_node_property: str,
        nodes: Iterable[Union[ogm.Model, py2neo.Node]] = None,
        inner_only=False,
    ) -> pd.DataFrame:
        """
        Find all :class:`py2neo.Relationship` objects representing given relationship available in the graph
        and construct a `pandas.DataFrame` table with their nodes. A single row in returned table describes a single
        relationship. For each matching relationship between nodes S and E use S[from_node_property] and
        E[to_node_property] as the corresponding row values.

        If `nodes` argument is used construct the table only with relationships where one of the
        nodes is provided in the parameter.
        If `nodes` is used and `inner_only` is True use only relationships where both nodes are
        provided in `nodes`.

        :param relationship: name of the relationship which objects should be used to construct the table.
        :type relationship: str
        :param from_node_property: Name of relationship's start node property that should be used in the constructed
            table in a row for corresponding relationship object.
        :type from_node_property: str
        :param to_node_property: Name of relationship's end node property that should be used in the constructed
            table in a row for corresponding relationship object.
        :type to_node_property: str
        :parm nodes: Iterable of either :class:`ogm.Model` or :class:`py2neo.Node` nodes that should be start/end
            node of used :class:`py2neo.Relationship` objects. If `inner_only` is True both start and end nodes
            of a relationship must be provided in `nodes` to include such relationship in the returned table.
        :type nodes: Iterable[Union[:class:`ogm.Model`, :class:`py2neo.Node`]]
        :param inner_only: Boolean value determining whether both start and end nodes of a single :class:`py2neo.Relationship`
            object should be available in `nodes`.
        :return: :class:`pandas.DataFrame` table that rows represent the available relationship objects in the graph.
        """
        relationship_objects = self.get_relationships(relationship, nodes, inner_only)
        relationship_ids = []
        for rel in relationship_objects:
            form_node, to_node = rel.nodes
            relationship_ids.append((form_node[from_node_property], to_node[to_node_property]))
        if from_node_property == to_node_property:
            from_node_property = f"{from_node_property}_from"
            to_node_property = f"{to_node_property}_to"
        return pd.DataFrame(relationship_ids, columns=[from_node_property, to_node_property])

    def get_relationships_for_dataframe(
        self,
        df: pd.DataFrame,
        relationship: str,
        from_model_class: Union[ogm.Model, str],
        to_model_class: Union[ogm.Model, str],
        from_key_column: str,
        to_key_column: str,
        from_model_id_key: str = None,
        to_model_id_key: str = None,
    ) -> pd.DataFrame:
        """
        Map relationships described by `df` table with :class:`py2neo.Relationship` objects available in the
        graph.

        This method is useful if one has created the :class:`py2neo.Relationship` objects based on a DataFrame
        values (e.g. with :meth:`PandasGraph.create_relationships_from_dataframe` method) and wants to retreive
        these objects in another execution.

        :param df: table describing the relationships that should be mapped with :class:`py2neo.Relationship`
            objects in the graph.
        :type df: :class:`pandas.DataFrame`
        :param relationship: name of the relationship that should be matched.
        :type relationship: str
        :param from_model_class: either :class:`ogm.Model` or string with name of label determining nodes
            that should be used to map start nodes of the relationship.
        :type from_model_class: Union[:class:`ogm.Model`, str]
        :param to_model_class: either :class:`ogm.Model` or string with name of label determining nodes
            that should be used to map end nodes of the relationship.
        :type to_model_class: Union[:class:`ogm.Model`, str]
        :param from_key_column: name of `df` table's column containing values that should be used to match
            the start node of each relationship
        :type from_key_column: str
        :param to_key_column: name of `df` table's column containing values that should be used to match
            the end node of each relationship
        :type to_key_column: str
        :param from_model_id_key: name of property that should be used to find matching start nodes in the graph.
            If `from_model_class` is a :class:`ogm.Model` subclass this parameter can be omitted and the
            `__primarykey__` of the class will be used.
        :type from_model_id_key: str, optional
        :param to_model_id_key: name of property that should be used to find matching end nodes in the graph.
            If `to_model_class` is a :class:`ogm.Model` subclass this parameter can be omitted and the
            `__primarykey__` of the class will be used.
        :type to_model_id_key: str, optional
        :return: :class:`pandas.DataFrame` table where a single row contains df[from_key_column], df[to_key_column]
            and :class:`py2neo.Relationship` representing corresponding relationship.
        """
        if from_model_id_key is None:
            if isinstance(from_model_class, str):
                raise InvalidArgumentsConfigurationError(
                    f"If `from_model_class` is string ('{from_model_class}' provided) it is assumed to be "
                    "the label of a `py2neo.Node` object and `from_model_id_key` must be defined to match the node."
                )
            from_model_id_key = from_model_class.__primarykey__

        if to_model_id_key is None:
            if isinstance(to_model_class, str):
                raise InvalidArgumentsConfigurationError(
                    f"If `to_model_id_key` is string ('{to_model_id_key}' provided) it is assumed to be "
                    "the label of a `py2neo.Node` object and `from_model_id_key` must be defined to match the node."
                )
            to_model_id_key = to_model_class.__primarykey__

        def _match_relationship(row: pd.Series):
            from_model_id = row[from_key_column]
            to_model_id = row[to_key_column]
            from_model_instance = self._match_model(
                from_model_class, **{from_model_id_key: from_model_id}
            )
            to_model_instance = self._match_model(to_model_class, **{to_model_id_key: to_model_id})
            if from_model_instance is None:
                raise NodeWithIdDoesNotExistError(from_model_class, from_model_id)
            if to_model_instance is None:
                raise NodeWithIdDoesNotExistError(to_model_class, to_model_id)
            from_node = (
                from_model_instance
                if isinstance(from_model_instance, py2neo.Node)
                else from_model_instance.__node__
            )
            to_node = (
                to_model_instance
                if isinstance(to_model_instance, py2neo.Node)
                else to_model_instance.__node__
            )
            relationship_object = self._relationship_matcher.match(
                nodes=[from_node, to_node], r_type=relationship
            ).first()
            if relationship_object is None:
                raise RelationshipDoesNotExistError(relationship, from_node, to_node)
            return relationship_object

        relationship_df = df[[from_key_column, to_key_column]]
        relationship_df[relationship] = relationship_df.apply(_match_relationship, axis=1)
        return relationship_df
