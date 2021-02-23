class Pandas2Neo4jError(Exception):
    pass


class NotSupportedModelClassError(Pandas2Neo4jError):
    pass


class NodeWithIdDoesNotExistsError(Pandas2Neo4jError):
    def __init__(self, node_class=None, node_id=None):
        self.node_class = node_class
        self.node_id = node_id

    def __str__(self):
        if self.node_class is  None or self.node_id is  None:
            return "Could not find requested node in the graph."
        return f"Node of {self.node_class} class with id value {self.node_id} was not found in the graph."
