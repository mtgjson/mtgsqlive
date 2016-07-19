#!/usr/bin/env bash

./json_to_sql.py 1 ~/Desktop/Real.db ~/Desktop/AllSets-x.json;
./sql_to_json.py ~/Desktop/Real.db ~/Desktop/MyOutput.json sets;
./json_to_sql.py 1 ~/Desktop/Mine.db ~/Desktop/MyOutput.json;
./sql_to_json.py ~/Desktop/Mine.db ~/Desktop/MySecondOutput.json sets;
diff ~/Desktop/MyOutput.json ~/Desktop/MySecondOutput.json > ~/Desktop/Diff.txt;
md5sum ~/Desktop/MyOutput.json ~/Desktop/MySecondOutput.json