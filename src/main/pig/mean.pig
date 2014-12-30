
REGISTER '$udf';

DEFINE mean com.inmobi.sampling.Mean('15');

A = Load '$input' using PigStorage('\u0001') AS (rq_id : chararray,impid : chararray,cl_id : chararray,rq_udid : chararray,rq_normalized_user_id : chararray,rq_siteid : chararray,rq_ccid : chararray,rq_geo_city : chararray,rq_handsetid : chararray,rq_intg_method : chararray,rq_loc_src : chararray,rq_mkvldadreq : int,dl_joined_count : int,dl_duplicate_count : int,cl_fraud : chararray,bl_cpc : double,bl_pubcpc : double,rq_adimp : int);

P = foreach (group A all) generate mean(A);

dump P;

