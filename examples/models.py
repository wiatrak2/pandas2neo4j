import pandas2neo4j
from pandas2neo4j import properties
from py2neo import ogm


class Address(pandas2neo4j.PandasModel):
    __primarykey__ = "uuid"

    uuid = properties.IntegerProperty(not_null=True)
    country = properties.StringProperty()
    city = properties.StringProperty()
    street_address = properties.StringProperty()
    lat = properties.FloatProperty()
    lon = properties.FloatProperty()

    authors = ogm.RelatedTo("People", "ADDRESS")
    address = ogm.RelatedTo("Publication", "AFFILIATION")


class Person(pandas2neo4j.PandasModel):
    __primarykey__ = "uuid"

    uuid = properties.IntegerProperty(not_null=True)
    firstname = properties.StringProperty(not_null=True)
    lastname = properties.StringProperty(not_null=True)
    company = properties.StringProperty()
    email = properties.StringProperty()
    phone_number = properties.StringProperty()

    author = ogm.RelatedTo("Publication", "AUTHOR")
    address = ogm.RelatedFrom("Address", "ADDRESS")


class Publication(pandas2neo4j.PandasModel):
    __primarykey__ = "uuid"

    uuid = properties.IntegerProperty(not_null=True)
    title = properties.StringProperty(not_null=True)
    year = properties.IntegerProperty()
    url = properties.StringProperty()

    authors = ogm.RelatedFrom("People", "AUTHOR")
    address = ogm.RelatedFrom("Address", "AFFILIATION")
