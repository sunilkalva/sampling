
A = load '/data/suc-*.log' as (siteid:chararray, count:int);
B = group(group A by siteid) generate group as siteid, SUM(A.count) as count;
store B into '/output/suc-agg.log' using PigStorage(",");