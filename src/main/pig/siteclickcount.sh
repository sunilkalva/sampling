#!/bin/sh

rm -rf /output/siteclid
rm -rf /output/siteclcount
./bin/pig -x local -f /Users/sunil.kalva/workspace/Learnings/samplings/src/main/pig/siteclickcount.pig -param input="/data/raw.log" -param output="/output/siteclcount"
