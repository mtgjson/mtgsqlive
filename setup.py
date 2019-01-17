"""Installation setup for MTGSQLite."""

import setuptools

# Necessary for TOX
setuptools.setup(
    name="MTGSQLite",
    version=0.1,
    author="Zach Halpern",
    author_email="Zahalpern+github@gmail.com",
    url="https://github.com/mtgjson/mtgsqlive/",
    description="Convert MTGJSONv4 files to SQLite format for Magic: The Gathering",
    long_description=open("README.md", "r").read(),
    long_description_content_type="text/markdown",
    license="GPL-3.0",
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
    ],
    keywords="Magic: The Gathering, MTG, JSON, Card Games, Collectible, Trading Cards",
    packages=setuptools.find_packages(),
    install_requires=["requests"],
)
