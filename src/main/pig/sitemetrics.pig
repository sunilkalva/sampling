REGISTER /jars/network-hygiene-1.1.11-SNAPSHOT.jar;
REGISTER /jars/activation-1.1.jar;
REGISTER /jars/antlr-2.7.7.jar;
REGISTER /jars/antlr-3.4.jar;
REGISTER /jars/antlr-runtime-3.5.jar;
REGISTER /jars/asm-3.2.jar;
REGISTER /jars/automaton-1.11-8.jar;
REGISTER /jars/avro-1.7.4.jar;
REGISTER /jars/bi-analytics-0.5.jar;
REGISTER /jars/commons-beanutils-1.7.0.jar;
REGISTER /jars/commons-beanutils-core-1.8.0.jar;
REGISTER /jars/commons-cli-1.2.jar;
REGISTER /jars/commons-codec-1.4.jar;
REGISTER /jars/commons-collections-3.2.1.jar;
REGISTER /jars/commons-compress-1.4.1.jar;
REGISTER /jars/commons-configuration-1.6.jar;
REGISTER /jars/commons-dbcp-1.4.jar;
REGISTER /jars/commons-digester-1.8.jar;
REGISTER /jars/commons-httpclient-3.1.jar;
REGISTER /jars/commons-lang-2.5.jar;
REGISTER /jars/commons-logging-1.1.1.jar;
REGISTER /jars/commons-logging-api-1.1.jar;
REGISTER /jars/commons-pool-1.5.4.jar;
REGISTER /jars/datanucleus-connectionpool-2.0.3.jar;
REGISTER /jars/datanucleus-core-2.0.3.jar;
REGISTER /jars/datanucleus-enhancer-2.0.3.jar;
REGISTER /jars/datanucleus-rdbms-2.0.3.jar;
REGISTER /jars/derby-10.6.1.0.jar;
REGISTER /jars/elephant-bird-core-4.5.jar;
REGISTER /jars/elephant-bird-hadoop-compat-4.5.jar;
REGISTER /jars/elephant-bird-pig-4.5.jar;
REGISTER /jars/gson-2.2.2.jar;
REGISTER /jars/guava-11.0.2.jar;
REGISTER /jars/hadoop-lzo-0.4.19.jar;
REGISTER /jars/hcatalog-core-0.5.0-cdh4.3.0.jar;
REGISTER /jars/hcatalog-pig-adapter-0.5.0-cdh4.3.0.jar;
REGISTER /jars/hive-builtins-0.10.0-cdh4.3.0.jar;
REGISTER /jars/hive-cli-0.10.0-cdh4.3.0.jar;
REGISTER /jars/hive-common-0.10.0-cdh4.3.0.jar;
REGISTER /jars/hive-contrib-0.10.0-cdh4.3.0.jar;
REGISTER /jars/hive-exec-0.10.0-cdh4.3.0.jar;
REGISTER /jars/hive-metastore-0.10.0-cdh4.3.0.jar;
REGISTER /jars/hive-serde-0.10.0-cdh4.3.0.jar;
REGISTER /jars/hive-service-0.10.0-cdh4.3.0.jar;
REGISTER /jars/hive-shims-0.10.0-cdh4.3.0.jar;
REGISTER /jars/hsqldb-1.8.0.10.jar;
REGISTER /jars/httpclient-4.1.3.jar;
REGISTER /jars/httpcore-4.1.3.jar;
REGISTER /jars/jackson-core-asl-1.8.8.jar;
REGISTER /jars/jackson-mapper-asl-1.8.8.jar;
REGISTER /jars/jansi-1.9.jar;
REGISTER /jars/JavaEWAH-0.3.2.jar;
REGISTER /jars/jdo2-api-2.3-ec.jar;
REGISTER /jars/jep-java-3.4.jar;
REGISTER /jars/jetty-util-6.1.26.cloudera.2.jar;
REGISTER /jars/jline-0.9.94.jar;
REGISTER /jars/joda-time-2.3.jar;
REGISTER /jars/jsch-0.1.42.jar;
REGISTER /jars/json-20140107.jar;
REGISTER /jars/json-simple-1.1.jar;
REGISTER /jars/jsr305-1.3.9.jar;
REGISTER /jars/jta-1.1.jar;
REGISTER /jars/jython-standalone-2.5.2.jar;
REGISTER /jars/krati-0.4.8.jar;
REGISTER /jars/leveldbjni-all-1.5.jar;
REGISTER /jars/libfb303-0.9.0.jar;
REGISTER /jars/libthrift-0.5.0.0.jar;
REGISTER /jars/libthrift-0.9.0.jar;
REGISTER /jars/log4j-1.2.16.jar;
REGISTER /jars/mockito-all-1.8.5.jar;
REGISTER /jars/oro-2.0.8.jar;
REGISTER /jars/paranamer-2.3.jar;
REGISTER /jars/pig-0.11.0-cdh4.3.0.jar;
REGISTER /jars/piggybank-0.12.1.jar;
REGISTER /jars/protobuf-java-2.4.1.jar;
REGISTER /jars/servlet-api-2.5.jar;
REGISTER /jars/slf4j-log4j12-1.6.1.jar;
REGISTER /jars/snappy-java-1.0.4.1.jar;
REGISTER /jars/ST4-4.0.4.jar;
REGISTER /jars/stringtemplate-3.2.1.jar;
REGISTER /jars/trove4j-3.0.3.jar;
REGISTER /jars/xz-1.0.jar;
REGISTER /jars/yoda-pig-loader-0.6.118.jar;
REGISTER /jars/zookeeper-3.4.5-cdh4.3.0.jar;

A = LOAD '$input' USING com.inmobi.dw.yoda.loader.PigLoader('rq_siteid,rq_id,impid,cl_id,bl_cpc')
AS (
  sid : chararray,
  rid : chararray,
  iid : chararray,
  cid :chararray,
  burn : double
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

store site_metrics into '/output/sitemetrics' using PigStorage('\u0001');