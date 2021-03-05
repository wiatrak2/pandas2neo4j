class Pandas2Neo4jError(Exception):
    pass


class NotSupportedModelClassError(Pandas2Neo4jError):
    pass


class NodeWithIdDoesNotExistsError(Pandas2Neo4jError):
    def __init__(self, node_class=None, node_id=None):
        self.node_class = node_class
        self.node_id = node_id

    def __str__(self):
        if self.node_class is None or self.node_id is None:
            return "Could not find requested node in the graph."
        return f"Node of {self.node_class} class with id value {self.node_id} was not found in the graph."


class PropertyValueWithInvalidTypeError(Pandas2Neo4jError):
    pass


class NotNullPropertyError(Pandas2Neo4jError):
    def __init__(self, property_instance):
        self.property_instance = property_instance

    def __str__(self):
        return (
            f"Property {self.property_instance.__class__.__name__} has `not_null` flag set to True.\n"
            f"None value was provided hovewer. Please use value with type {self.property_instance.TYPE}"
        )
