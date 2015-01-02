A = Load '$input' using PigStorage('\u0001') AS (rq_id : chararray,impid : chararray,cl_id : chararray,rq_udid : chararray,rq_normalized_user_id : chararray,rq_siteid : chararray,rq_ccid : chararray,rq_geo_city : chararray,rq_handsetid : chararray,rq_intg_method : chararray,rq_loc_src : chararray,rq_mkvldadreq : int,dl_joined_count : int,dl_duplicate_count : int,cl_fraud : chararray,bl_cpc : double,bl_pubcpc : double,rq_adimp : int);

counter = group A all;
grp        = group A by rq_siteid;

tmean = foreach grp {
        sum   = SUM(A.bl_cpc);
        count = COUNT(A);
        generate group as rq_siteid, (double) sum/count as mean, (double) count as count, 1 as x;
};

meanCount = foreach counter {
        generate flatten(group) , 1 as x, COUNT(A) as totalRecords;
}

TotalRecordJoin = join tmean by x, meanCount by x using 'replicated';

AverageRecordsBurn = foreach TotalRecordJoin generate (tmean::count/meanCount::totalRecords)*(tmean::mean) as dd;

final = group AverageRecordsBurn all ;

summ = foreach final generate SUM(AverageRecordsBurn.dd);

dump summ;

