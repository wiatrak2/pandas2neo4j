from typing import Any, Callable, Iterable, Tuple, Union

import pandas as pd
import py2neo
from py2neo import ogm
import numpy as np

from pandas2neo4j.pandas_model import PandasModel
from pandas2neo4j.errors import (
    NodeWithIdDoesNotExistsError,
    NotSupportedModelClassError,
)


class PandasGraph(ogm.Repository):
    @property
    def schema(self) -> py2neo.Schema:
        return self.graph.schema

    def create_graph_object(self, subgraph: Union[ogm.Model, py2neo.Entity]):
        self.graph.create(subgraph)

    def create_graph_objects(self, objects: Iterable[Union[ogm.Model, py2neo.Entity]]):
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
                    )
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
