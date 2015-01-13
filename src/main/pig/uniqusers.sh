#!/bin/sh

rm -rf /output/siteudid
rm -rf /output/siteuniqusers
./bin/pig -x local -f /Users/sunil.kalva/workspace/Learnings/samplings/src/main/pig/uniqusers.pig -param input="/data/raw.log" -param output="/output/siteuniqusers"
