CREATE TABLE supply_fact AS (
  rq_id STRING,
  impid STRING,
  cl_id STRING,
  rq_udid STRING,
  rq_normalized_user_id STRING,
  rq_siteid STRING,
  rq_ccid STRING,
  rq_geo_city STRING,
  rq_handsetid STRING,
  rq_intg_method STRING,
  rq_loc_src STRING,
  rq_mkvldadreq INT,
  dl_joined_count INT,
  dl_duplicate_count INT,
  cl_fraud STRING,
  bl_cpc DOUBLE,
  bl_pubcpc DOUBLE,
  rq_adimp INT
);


in = load 'hdfs://localhost:9000/sampledata/*'