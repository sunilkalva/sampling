X = Load '$input' using PigStorage('\u0001')
AS (
    rid : chararray,
    iid : chararray,
    cid : chararray,
    udid : chararray,
    rq_normalized_user_id : chararray,
    siteid : chararray,
    rq_ccid : chararray,
    rq_geo_city : chararray,
    rq_handsetid : chararray,
    rq_intg_method : chararray,
    rq_loc_src : chararray,
    rq_mkvldadreq : int,
    dl_joined_count : int,
    dl_duplicate_count : int,
    cl_fraud : chararray,
    burn : double,
    bl_pubcpc : double,
    rq_adimp : int
);

Y = foreach X generate siteid, udid;

A = filter Y by UPPER(udid) != 'NULL';

a_uniq = DISTINCT A;

STORE a_uniq INTO '/output/siteudid-1' using PigStorage('\u0001');

a_uniq_count = foreach(group a_uniq by siteid)  generate FLATTEN(group), COUNT(a_uniq);

STORE a_uniq_count INTO '$output' using PigStorage('\u0001');