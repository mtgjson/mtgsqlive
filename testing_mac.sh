#!/usr/bin/env bash

./json_to_sql.py ~/Desktop/original.db ~/Desktop/AllSets-x.json 1
./sql_to_json.py ~/Desktop/original.db ~/Desktop/FirstOutput.json "sets"
diff ~/Desktop/AllSets-x.json ~/Desktop/FirstOutput.json >> ~/Desktop/FirstRunDiff.txt
