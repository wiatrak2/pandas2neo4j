from typing import Any, Callable, Tuple, Union

import pandas as pd
import py2neo
from py2neo import ogm

from pandas2neo4j.pandas_model import PandasModel
from pandas2neo4j.errors import (
    NodeWithIdDoesNotExistsError,
    NotSupportedModelClassError,
)


class PandasGraph(ogm.Repository):
    def _get_node_from_model(
        self,
        model_class: ogm.Model,
        id_key: str,
        id_value: Any,
    ) -> py2neo.Node:
        del id_key
        model_instance = self.get(model_class, id_value)
        if model_instance is None:
            raise NodeWithIdDoesNotExistsError
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
            from_node_getter(
                from_model_class, from_model_id_key, row[from_key_column].item()
            ),
            to_node_getter(to_model_class, to_model_id_key, row[to_key_column].item()),
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

    def _save_graph_objects(self, objects: pd.Series):
        tx = self.graph.begin()
        for obj in objects:
            tx.create(obj)
        tx.commit()

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
    ) -> pd.Series:
        relationships = df.apply(
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
                )
            ),
            axis=1,
        )
        self._save_graph_objects(relationships)
        return relationships

    def _create_graph_nodes_from_dataframe(
        self, df: pd.DataFrame, node_label: str
    ) -> pd.Series:
        nodes = df.apply(lambda row: py2neo.Node(node_label, **row), axis=1)
        self._save_graph_objects(nodes)
        return nodes

    def create_nodes_from_dataframe(
        self, df: pd.DataFrame, model_class: Union[ogm.Model, str]
    ) -> pd.Series:
        if isinstance(model_class, str):
            return self._create_graph_nodes_from_dataframe(df, model_class)
        elif issubclass(model_class, PandasModel):
            nodes = df.apply(model_class, axis=1)
        elif hasattr(model_class, "from_pandas_series"):
            nodes = df.apply(model_class.from_pandas_series, axis=1)
        else:
            raise NotSupportedModelClassError
        self.save(*nodes)
        return nodes
