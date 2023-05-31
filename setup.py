"""Installation setup for MTGSQLite."""

import pathlib
import setuptools

project_root: pathlib.Path = pathlib.Path(__file__).resolve().parent

# Necessary for TOX
setuptools.setup(
    name="MTGSQLite",
    version="0.3",
    author="Zach Halpern",
    author_email="zach@mtgjson.com",
    url="https://github.com/mtgjson/mtgsqlive/",
    description="Convert MTGJSONv5 JSON files into alternative formats for Magic: The Gathering",
    long_description=project_root.joinpath("README.md").open(encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows :: Windows 10",
        "Operating System :: Microsoft :: Windows :: Windows 11",
        "Operating System :: Unix",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Software Development :: Version Control :: Git",
    ],
    keywords="Magic: The Gathering, MTG, JSON, Card Games, Collectible, Trading Cards",
    include_package_data=True,
    packages=setuptools.find_packages(),
    install_requires=project_root.joinpath("requirements.txt")
    .open(encoding="utf-8")
    .readlines()
    if project_root.joinpath("requirements.txt").is_file()
    else [],  # Use the requirements file, if able
)
