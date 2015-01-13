#!/bin/sh

rm -rf /output/sitemetricsdata
rm -rf /output/sitemetrics
./bin/pig -x local -f /Users/sunil.kalva/workspace/Learnings/samplings/src/main/pig/sitemetrics.pig -param input="/data/raw.log" -param output="/output/sitemetrics"
