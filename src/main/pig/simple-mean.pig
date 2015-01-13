A = Load '$input' using PigStorage('\u0001') AS (rq_id : chararray,impid : chararray,cl_id : chararray,rq_udid : chararray,rq_normalized_user_id : chararray,rq_siteid : chararray,rq_ccid : chararray,rq_geo_city : chararray,rq_handsetid : chararray,rq_intg_method : chararray,rq_loc_src : chararray,rq_mkvldadreq : int,dl_joined_count : int,dl_duplicate_count : int,cl_fraud : chararray,bl_cpc : double,bl_pubcpc : double,rq_adimp : int);

counter = group A all;
grp        = group A by rq_siteid;

localMeanTuple = foreach grp {
        sum   = SUM(A.bl_cpc);
        groupCount = COUNT(A);
        generate group as rq_siteid, (double) sum/groupCount as localMean, (double) groupCount as groupCount, 1 as x;
};

meanCount = foreach counter {
        generate flatten(group) , 1 as x, COUNT(A) as totalCount;
}

TotalRecordJoin = join localMeanTuple by x, meanCount by x using 'replicated';

AverageRecordsBurn = foreach TotalRecordJoin generate (localMeanTuple::groupCount / meanCount::totalCount) * (localMeanTuple::localMean) as avg;

final = group AverageRecordsBurn all ;

finalMean = foreach final generate SUM(AverageRecordsBurn.avg);

dump finalMean;

