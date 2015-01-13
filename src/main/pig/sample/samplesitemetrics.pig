A = Load '$input' using PigStorage('\u0001')
AS (
    rid : chararray,
    iid : chararray,
    cid : chararray,
    rq_udid : chararray,
    rq_normalized_user_id : chararray,
    sid : chararray,
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

burn_table = foreach (group A by sid) generate group as sid, SUM(A.burn) as burn;

click_filter = filter A by UPPER(cid) != 'NULL';
click_table = foreach (group click_filter by sid) generate group as sid, COUNT(click_filter) as clicks;

impression_filter = filter A by iid != 'NULL';
impression_table = foreach (group impression_filter by sid) generate group as sid, COUNT(impression_filter) as impressions;

request_filter = filter A by rid != 'NULL';
request_table = foreach (group request_filter by sid) generate group as sid, COUNT(request_filter) as requests;

ctr_filter = join click_table by sid, impression_table by sid;
ctr_table = foreach ctr_filter generate click_table::sid as sid, (float)click_table::clicks/impression_table::impressions as ctr;

ri = join request_table by sid left outer, impression_table by sid;
ri_table = foreach ri generate request_table::sid as sid, request_table::requests as requests, impression_table::impressions as impressions;

ric = join ri_table by sid left outer, click_table by sid;
ric_table = foreach ric generate ri_table::sid as sid, ri_table::requests as requests, ri_table::impressions as impressions, click_table::clicks as clicks;

ricb = join ric_table by sid left outer, burn_table by sid;
ricb_table = foreach ricb generate ric_table::sid as sid, ric_table::requests as requests, ric_table::impressions as impressions, ric_table::clicks as clicks, burn_table::burn as burn;

ricbr = join ricb_table by sid left outer, ctr_table by sid;
site_metrics = foreach ricbr generate ricb_table::sid as sid, ricb_table::requests as requests, ricb_table::impressions as impressions, ricb_table::clicks as clicks, ricb_table::burn as burn, ctr_table::ctr as ctr;

store site_metrics into '$output' using PigStorage('\u0001');