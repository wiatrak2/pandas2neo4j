class Pandas2Neo4jError(Exception):
    pass


class NotSupportedModelClassError(Pandas2Neo4jError):
    pass


class NodeWithIdDoesNotExistsError(Pandas2Neo4jError):
    pass
