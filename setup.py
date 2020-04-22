"""Installation setup for MTGSQLite."""

import pathlib
import setuptools

# Necessary for TOX
setuptools.setup(
    name="MTGSQLite",
    version=0.2,
    author="Zach Halpern",
    author_email="zach@mtgjson.com",
    url="https://github.com/mtgjson/mtgsqlive/",
    description="Convert MTGJSONv4 and MTGJSONv5 files to SQLite format for Magic: The Gathering",
    long_description=pathlib.Path("README.md").open().read(),
    long_description_content_type="text/markdown",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT",
    ],
    keywords="Magic: The Gathering, MTG, JSON, Card Games, Collectible, Trading Cards",
    packages=setuptools.find_packages(),
    install_requires=pathlib.Path("requirements.txt").open().readlines(),
)
