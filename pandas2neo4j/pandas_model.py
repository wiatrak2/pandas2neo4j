from typing import Any, Dict, List, Iterable

import pandas as pd
import py2neo
from py2neo import ogm


class PandasModel(ogm.Model):
    def __init__(self, row: pd.Series):
        for column in [col for col in row.keys() if hasattr(self, col)]:
            self.__setattr__(column, row[column])

    def to_dict(self, properties: List[str] = None) -> Dict[str, Any]:
        if properties is None:
            return dict(self.__node__.items())
        return {p: getattr(self, p) for p in properties}


def models_to_dataframe(models: Iterable[PandasModel], columns: List[str] = None) -> pd.DataFrame:
    return pd.DataFrame((m.to_dict(columns) for m in models))


def nodes_to_dataframe(nodes: Iterable[py2neo.Node], columns: List[str] = None) -> pd.DataFrame:
    nodes_properties = []
    for node in nodes:
        node_dict = dict(node.items())
        if columns is not None:
            node_dict = {k: node_dict[k] for k in columns}
        nodes_properties.append(node_dict)
    return pd.DataFrame(nodes_properties)
