import py2neo
import numpy as np

from pandas2neo4j.errors import PropertyValueWithInvalidTypeError, NotNullPropertyError


class SchemaProperty(py2neo.ogm.Property):
    """
    Extension of :class:`ogm.Property` that supports specifying a property's type.

    If initialized with `not_null=True` a :class:`ogm.Model`/:class:`.PandasModel`
    cannot be created with `NaN`/`None` value for the property. It can however still be
    initialized without specifying the property, but such a restriction is especially
    useful when creating model instances based on `pandas.DataFrame` rows, where some
    values may not be provided, so `NaN` would be used.
    """
    TYPE = None

    def __init__(self, cast_value=True, not_null=False, key=None, default=None):
        super().__init__(key, default)
        self.cast_value = cast_value
        self.not_null = not_null

    def __get__(self, *args, **kwargs):
        value = super().__get__(*args, **kwargs)
        if value is not None and type(value) is not self.TYPE:
            raise PropertyValueWithInvalidTypeError(
                f"Property requires values with {self.TYPE} type.\n"
                f"Got value {value} with type {type(value)} instead."
            )
        return value

    def __set__(self, instance, value):
        if value is not None and self.cast_value and type(value) != self.TYPE:
            try:
                if type(value) is float and np.isnan(value):
                    value = None
                else:
                    value = self.TYPE(value)
            except (TypeError, ValueError):
                raise PropertyValueWithInvalidTypeError(
                    f"Property requires {self.TYPE} type values.\n"
                    f"Got {value} of type {type(value)} instead and failed to cast it to {self.TYPE}."
                )

        if value is not None and type(value) is not self.TYPE:
            raise PropertyValueWithInvalidTypeError(
                f"Property requires values with {self.TYPE} type.\n"
                f"Tried to set value {value} with type {type(value)} instead."
            )

        if self.not_null:
            self._validate_not_null(value)

        super().__set__(instance, value)

    def _validate_not_null(self, value):
        if value is None or (type(value) is float and np.isnan(value)):
            raise NotNullPropertyError(self)


class StringProperty(SchemaProperty):
    """
    Property storing `string` object.
    """
    TYPE = str


class IntegerProperty(SchemaProperty):
    """
    Property storing `int` object.
    """
    TYPE = int


class FloatProperty(SchemaProperty):
    """
    Property storing `float` object.
    """
    TYPE = float


class BooleanProperty(SchemaProperty):
    """
    Property storing `bool` object.
    """
    TYPE = bool


class ListProperty(SchemaProperty):
    """
    Property storing `list` object. `nested_type` determines type of the list's elements
    (lists with elements of various type are not supported.)
    """
    TYPE = list

    def __init__(self, nested_type, *args, **kwargs):
        self.nested_type = nested_type
        super().__init__(*args, **kwargs)

    def __get__(self, *args, **kwargs):
        value = super(SchemaProperty, self).__get__(*args, **kwargs)
        if value is not None:
            if type(value) is not list:
                raise PropertyValueWithInvalidTypeError(
                    "Property requires list values.\n"
                    f"Has value {value} of type {type(value)} instead"
                )
            if any((type(elem) is not self.nested_type for elem in value)):
                raise PropertyValueWithInvalidTypeError(
                    f"Property requires list elements with {self.TYPE} type.\n"
                    f"One of list {value} elements is invalid."
                )
        return value

    def __set__(self, instance, value):
        if value is not None and self.cast_value and type(value) != self.TYPE:
            try:
                if type(value) is float and np.isnan(value):
                    value = None
                else:
                    value = [self.nested_type(elem) for elem in value]
            except (TypeError, ValueError):
                raise PropertyValueWithInvalidTypeError(
                    f"Property requires list values with elements of type {self.nested_type}.\n"
                    f"Failed when casting {value}."
                )

        if (
            value is not None
            and type(value) is not list
            or any((type(elem) is not self.nested_type for elem in value))
        ):
            raise PropertyValueWithInvalidTypeError(
                f"Property requires list values with elements of type {self.TYPE}.\n"
                f"Tried to set value {value} instead."
            )

        if self.not_null:
            self._validate_not_null(value)

        super(SchemaProperty, self).__set__(instance, value)
