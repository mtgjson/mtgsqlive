# MTGSQLive

[![Join the chat at https://gitter.im/mtgjson/mtgsqlive](https://badges.gitter.im/mtgjson/mtgsqlive.svg)](https://gitter.im/mtgjson/mtgsqlive)

The goal of this project is to create a SQLite database with all Magic: The Gathering card data that is supported by Gatherer and [MTGJSON](https://mtgjson.com).

We don't like being dependent on Gatherer for card data, as their update time is always delayed and there are sometimes obvious and egregious mistakes that we can correct in real time.

The database starts as a copy of the MTGJSON source of AllSets-x.json, then we manually go in to correct any errors, as MTGJSON is a direct parse of Gatherer. When spoiler season comes around, we can manually update this (with a tool to be developed later) so we will always have a database that's up to date. Projects can pull in our complete database for their projects in order to have a full Magic: the Gathering card database!