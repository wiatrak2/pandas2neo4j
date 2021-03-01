import py2neo

from pandas2neo4j.errors import PropertyValueWithInvalidTypeError


class SchemaProperty(py2neo.ogm.Property):
    TYPE = None

    def __init__(self, cast_value=False, key=None, default=None):
        super().__init__(key, default)
        self.cast_value = cast_value

    def __get__(self, *args, **kwargs):
        value = super().__get__(*args, **kwargs)
        if value is not None and type(value) is not self.TYPE:
            raise PropertyValueWithInvalidTypeError(
                f"Property requires values with {self.TYPE} type.\n"
                f"Got value {value} with type {type(value)} instead."
            )
        return value

    def __set__(self, instance, value):
        if self.cast_value and type(value) != self.TYPE:
            try:
                value = self.TYPE(value)
            except TypeError:
                raise PropertyValueWithInvalidTypeError(
                    f"Property requires {self.TYPE} type values.\n"
                    f"Got {value} of type {type(value)} instead and failed to cast it to {self.TYPE}."
                )
        if type(value) != self.TYPE:
            raise PropertyValueWithInvalidTypeError(
                f"Property requires values with {self.TYPE} type.\n"
                f"Tried to set value {value} with type {type(value)} instead."
            )
        super().__set__(instance, value)


class StringProperty(SchemaProperty):
    TYPE = str


class IntegerProperty(SchemaProperty):
    TYPE = int


class FloatProperty(SchemaProperty):
    TYPE = float
