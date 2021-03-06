from setuptools import setup, find_packages

setup(
    name="pandas2neo4j",
    description="Convert pandas DataFrames to neo4j graph",
    version="0.0.1dev",
    author=u"Wojciech Pratkowiecki",
    author_email="wpratkowiecki@gmail.com",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "py2neo>=2021.0.0",
        "pandas>=1.1.0,<2",
        "numpy>=1.18.0,<2",
        "cached-property>=1.5.2,<2",
    ],
    python_requires=">=3.7",
)
