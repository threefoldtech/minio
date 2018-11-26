```
➜  failures_testsuite git:(s3_redundant_controller) ✗ nosetests-3.4 -vs --logging-level=WARNING --progressive-with-bar --rednose testcases/s3
[Mon26 14:03] - base_test.py      :89  :j.s3_failures        - INFO     - s3_demo_5 url : {'public': 'http://172.27.24.226:1024', 'storage': 'http://10.102.189.153:1025'}
[Mon26 14:03] - failures.py       :34  :j.<module>           - INFO     - check a14ced03-451f-4564-a17d-f90feabdec37 on node ac1f6b4571d0 status
[Mon26 14:03] - failures.py       :34  :j.<module>           - INFO     - check a9781952-cefd-48e9-a29a-981a64f0e717 on node ac1f6b4571d8 status
[Mon26 14:03] - failures.py       :34  :j.<module>           - INFO     - check d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4 status
[Mon26 14:03] - failures.py       :36  :j.<module>           - INFO     - status : ok
[Mon26 14:03] - failures.py       :36  :j.<module>           - INFO     - status : ok
[Mon26 14:03] - failures.py       :36  :j.<module>           - INFO     - status : ok
[Mon26 14:03] - failures.py       :284 :j.<module>           - INFO     - check status tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on node ac1f6b272370
[Mon26 14:03] - failures.py       :286 :j.<module>           - INFO     - status : running ok
test001_upload_stop_tlog_start_download ... [Mon26 14:03] - s3.py             :43  :j.s3demo             - INFO     - get s3 client : ParseResult(scheme='http', netloc='172.27.24.226:1024', path='', params='', query='', fragment='')
[Mon26 14:03] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:03] - s3.py             :172 :j.s3demo             - INFO     - bucket : epd1gt3xfur6c4wd
[Mon26 14:03] - s3.py             :190 :j.s3demo             - INFO     - upload em06ar60dwk2aw30 file to epd1gt3xfur6c4wd bucket
[Mon26 14:04] - failures.py       :257 :j.<module>           - INFO     - tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on e5b1f889-ab19-40b1-836b-594cee04cdb5 node
[Mon26 14:04] - failures.py       :258 :j.<module>           - INFO     - tlog namespace e5b1f889-ab19-40b1-836b-594cee04cdb5 on e5b1f889-ab19-40b1-836b-594cee04cdb5 node
[Mon26 14:04] - failures.py       :259 :j.<module>           - INFO     - check status tlog zbd
[Mon26 14:04] - failures.py       :261 :j.<module>           - INFO     - status : running ok
[Mon26 14:04] - failures.py       :262 :j.<module>           - INFO     - stop tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on node ac1f6b272370
[Mon26 14:05] - s3.py             :202 :j.s3demo             - INFO     - download em06ar60dwk2aw30 file form epd1gt3xfur6c4wd bucket
[Mon26 14:05] - failures.py       :284 :j.<module>           - INFO     - check status tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on node ac1f6b272370
[Mon26 14:05] - failures.py       :288 :j.<module>           - INFO     - status : not running
[Mon26 14:05] - failures.py       :289 :j.<module>           - INFO     - start tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on node ac1f6b272370
[Mon26 14:05] - test01_tlog.py    :26  :j.s3_failures        - INFO     - wait till tlog  be up
[Mon26 14:05] - s3.py             :202 :j.s3demo             - INFO     - download em06ar60dwk2aw30 file form epd1gt3xfur6c4wd bucket
passed
test002_stop_tlog_upload_download ... [Mon26 14:05] - failures.py       :257 :j.<module>           - INFO     - tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on e5b1f889-ab19-40b1-836b-594cee04cdb5 node
[Mon26 14:05] - failures.py       :258 :j.<module>           - INFO     - tlog namespace e5b1f889-ab19-40b1-836b-594cee04cdb5 on e5b1f889-ab19-40b1-836b-594cee04cdb5 node
[Mon26 14:05] - failures.py       :259 :j.<module>           - INFO     - check status tlog zbd
[Mon26 14:05] - failures.py       :261 :j.<module>           - INFO     - status : running ok
[Mon26 14:05] - failures.py       :262 :j.<module>           - INFO     - stop tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on node ac1f6b272370
[Mon26 14:07] - test01_tlog.py    :49  :j.s3_failures        - INFO     - upload file, should fail
[Mon26 14:19] - s3.py             :178 :j.s3demo             - ERROR    - can't create bucket!
[Mon26 14:19] - s3.py             :179 :j.s3demo             - ERROR    - HTTPConnectionPool(host='172.27.24.226', port=1024): Max retries exceeded with url: /7ylrfm5lpszf56cv/ (Caused by ReadTimeoutError("HTTPConnectionPool(host='172.27.24.226', port=1024): Read timed out. (read timeout=<object object at 0x7f418b9b72f0>)",))
[Mon26 14:19] - failures.py       :284 :j.<module>           - INFO     - check status tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on node ac1f6b272370
[Mon26 14:19] - failures.py       :288 :j.<module>           - INFO     - status : not running
[Mon26 14:19] - failures.py       :289 :j.<module>           - INFO     - start tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on node ac1f6b272370
[Mon26 14:19] - test01_tlog.py    :63  :j.s3_failures        - INFO     - upload file, should success
[Mon26 14:20] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:20] - s3.py             :172 :j.s3demo             - INFO     - bucket : qbzswphen06ds2yy
[Mon26 14:20] - s3.py             :190 :j.s3demo             - INFO     - upload qouogo82cg8hxg7x file to qbzswphen06ds2yy bucket
[Mon26 14:20] - test01_tlog.py    :65  :j.s3_failures        - INFO     - download file, should success
[Mon26 14:20] - s3.py             :202 :j.s3demo             - INFO     - download qouogo82cg8hxg7x file form qbzswphen06ds2yy bucket
passed
test003_upload_kill_tlog_download ... [Mon26 14:20] - test01_tlog.py    :78  :j.s3_failures        - INFO     - Upload file
[Mon26 14:20] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:20] - s3.py             :172 :j.s3demo             - INFO     - bucket : 2cqa9kdq2uqm0lfw
[Mon26 14:20] - s3.py             :190 :j.s3demo             - INFO     - upload 2jbpnddr4ax0q5ka file to 2cqa9kdq2uqm0lfw bucket
[Mon26 14:21] - failures.py       :329 :j.<module>           - INFO     - kill tlog zdb process, zrobot will bring it back
[Mon26 14:21] - failures.py       :339 :j.<module>           - INFO     - kill ac361150-1ebc-4bb7-8e2b-de15d0cdae8e tlog zdb
[Mon26 14:22] - test01_tlog.py    :84  :j.s3_failures        - INFO     - Download file, should succeed
[Mon26 14:22] - s3.py             :202 :j.s3demo             - INFO     - download 2jbpnddr4ax0q5ka file form 2cqa9kdq2uqm0lfw bucket
passed
test004_kill_tlog_upload_download ... [Mon26 14:22] - failures.py       :329 :j.<module>           - INFO     - kill tlog zdb process, zrobot will bring it back
[Mon26 14:22] - failures.py       :339 :j.<module>           - INFO     - kill ac361150-1ebc-4bb7-8e2b-de15d0cdae8e tlog zdb
[Mon26 14:23] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:23] - s3.py             :172 :j.s3demo             - INFO     - bucket : q9tmu6n9vgufxq3i
[Mon26 14:23] - s3.py             :190 :j.s3demo             - INFO     - upload h21jccsp529azrpg file to q9tmu6n9vgufxq3i bucket
[Mon26 14:24] - s3.py             :202 :j.s3demo             - INFO     - download h21jccsp529azrpg file form q9tmu6n9vgufxq3i bucket
passed
[Mon26 14:24] - base_test.py      :89  :j.s3_failures        - INFO     - s3_demo_5 url : {'public': 'http://172.27.24.226:1024', 'storage': 'http://10.102.189.153:1025'}
[Mon26 14:24] - failures.py       :34  :j.<module>           - INFO     - check a14ced03-451f-4564-a17d-f90feabdec37 on node ac1f6b4571d0 status
[Mon26 14:24] - failures.py       :34  :j.<module>           - INFO     - check a9781952-cefd-48e9-a29a-981a64f0e717 on node ac1f6b4571d8 status
[Mon26 14:24] - failures.py       :34  :j.<module>           - INFO     - check d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4 status
[Mon26 14:24] - failures.py       :36  :j.<module>           - INFO     - status : ok
[Mon26 14:24] - failures.py       :36  :j.<module>           - INFO     - status : ok
[Mon26 14:24] - failures.py       :36  :j.<module>           - INFO     - status : ok
^Bd[Mon26 14:24] - failures.py       :284 :j.<module>           - INFO     - check status tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on node ac1f6b272370
[Mon26 14:24] - failures.py       :286 :j.<module>           - INFO     - status : running ok
test001_zdb_kill ... [Mon26 14:24] - s3.py             :43  :j.s3demo             - INFO     - get s3 client : ParseResult(scheme='http', netloc='172.27.24.226:1024', path='', params='', query='', fragment='')
[Mon26 14:24] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:24] - s3.py             :172 :j.s3demo             - INFO     - bucket : h0snro8bn7h31gtl
[Mon26 14:24] - s3.py             :190 :j.s3demo             - INFO     - upload pqda91btwyad084d file to h0snro8bn7h31gtl bucket
[Mon26 14:24] - test02_zdb.py     :16  :j.s3_failures        - INFO     - kill zdb process and make sure it will restart automatically
[Mon26 14:24] - failures.py       :113 :j.<module>           - INFO     - kill d5417b47-c244-4037-9641-9ce0e173e128 zdb process on node ac1f6b4573d4
[Mon26 14:24] - failures.py       :122 :j.<module>           - INFO     - wait zdb process to restart. 
[Mon26 14:25] - failures.py       :129 :j.<module>           - INFO     - zdb took 10.59549593925476 sec to restart
[Mon26 14:25] - s3.py             :202 :j.s3demo             - INFO     - download pqda91btwyad084d file form h0snro8bn7h31gtl bucket
passed
test002_upload_stop_parity_zdb_download ... [Mon26 14:25] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:25] - s3.py             :172 :j.s3demo             - INFO     - bucket : vtth3e296230nbfy
[Mon26 14:25] - s3.py             :190 :j.s3demo             - INFO     - upload vkv70fj2qfzx7wob file to vtth3e296230nbfy bucket
[Mon26 14:25] - s3.py             :202 :j.s3demo             - INFO     - download vkv70fj2qfzx7wob file form vtth3e296230nbfy bucket
[Mon26 14:25] - test02_zdb.py     :40  :j.s3_failures        - INFO     - Stop 1 zdb
[Mon26 14:25] - failures.py       :155 :j.<module>           - INFO     - stop d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4
[Mon26 14:25] - s3.py             :202 :j.s3demo             - INFO     - download vkv70fj2qfzx7wob file form vtth3e296230nbfy bucket
[Mon26 14:26] - test02_zdb.py     :46  :j.s3_failures        - INFO     - Start 1 zdb
[Mon26 14:26] - failures.py       :185 :j.<module>           - INFO     - start d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4
passed
test003_stop_parity_zdb_upload_download_start ... [Mon26 14:26] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:26] - s3.py             :172 :j.s3demo             - INFO     - bucket : hepoordrvg840pdr
[Mon26 14:26] - s3.py             :190 :j.s3demo             - INFO     - upload k1efgxietfud8ckn file to hepoordrvg840pdr bucket
[Mon26 14:26] - test02_zdb.py     :60  :j.s3_failures        - INFO     - stop 1 zdb
[Mon26 14:26] - failures.py       :155 :j.<module>           - INFO     - stop d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4
[Mon26 14:26] - s3.py             :202 :j.s3demo             - INFO     - download k1efgxietfud8ckn file form hepoordrvg840pdr bucket
[Mon26 14:26] - test02_zdb.py     :66  :j.s3_failures        - INFO     - start 1 zdb
[Mon26 14:26] - failures.py       :185 :j.<module>           - INFO     - start d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4
passed
test004_stop_parity_zdb_upload_start_download ... [Mon26 14:26] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:26] - s3.py             :172 :j.s3demo             - INFO     - bucket : 62eldu6jlfov3oiy
[Mon26 14:26] - s3.py             :190 :j.s3demo             - INFO     - upload hbwa1on974mckiwc file to 62eldu6jlfov3oiy bucket
[Mon26 14:27] - test02_zdb.py     :80  :j.s3_failures        - INFO     - stop 1 zdb
[Mon26 14:27] - failures.py       :155 :j.<module>           - INFO     - stop d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4
[Mon26 14:27] - test02_zdb.py     :83  :j.s3_failures        - INFO     - start 1 zdb
[Mon26 14:27] - failures.py       :185 :j.<module>           - INFO     - start d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4
[Mon26 14:27] - s3.py             :202 :j.s3demo             - INFO     - download hbwa1on974mckiwc file form 62eldu6jlfov3oiy bucket
passed
test005_stop_greater_parity_zdb_upload ... [Mon26 14:27] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:27] - s3.py             :172 :j.s3demo             - INFO     - bucket : ubhmembdt1tewrre
[Mon26 14:27] - s3.py             :190 :j.s3demo             - INFO     - upload 7gwp7k835qrnbhu5 file to ubhmembdt1tewrre bucket
[Mon26 14:27] - test02_zdb.py     :101 :j.s3_failures        - INFO     - stop 2 zdb
[Mon26 14:27] - failures.py       :155 :j.<module>           - INFO     - stop d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4
[Mon26 14:28] - failures.py       :155 :j.<module>           - INFO     - stop a9781952-cefd-48e9-a29a-981a64f0e717 on node ac1f6b4571d8
[Mon26 14:28] - test02_zdb.py     :104 :j.s3_failures        - INFO     - uploading should fail
[Mon26 14:28] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:28] - s3.py             :172 :j.s3demo             - INFO     - bucket : qzpx2a6inigtfbcj
[Mon26 14:28] - s3.py             :190 :j.s3demo             - INFO     - upload wwy19ogu5tz7uwdq file to qzpx2a6inigtfbcj bucket
[Mon26 14:31] - s3.py             :194 :j.s3demo             - WARNING  - Can't upload wwy19ogu5tz7uwdq file
[Mon26 14:31] - s3.py             :195 :j.s3demo             - ERROR    - HTTPConnectionPool(host='172.27.24.226', port=1024): Max retries exceeded with url: /qzpx2a6inigtfbcj/wwy19ogu5tz7uwdq (Caused by ResponseError('too many 500 error responses',))
[Mon26 14:31] - test02_zdb.py     :108 :j.s3_failures        - INFO     - start 2 zdb
[Mon26 14:31] - failures.py       :185 :j.<module>           - INFO     - start d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4
[Mon26 14:31] - failures.py       :185 :j.<module>           - INFO     - start a9781952-cefd-48e9-a29a-981a64f0e717 on node ac1f6b4571d8
[Mon26 14:31] - s3.py             :202 :j.s3demo             - INFO     - download 7gwp7k835qrnbhu5 file form ubhmembdt1tewrre bucket
passed
[Mon26 14:31] - base_test.py      :89  :j.s3_failures        - INFO     - s3_demo_5 url : {'public': 'http://172.27.24.226:1024', 'storage': 'http://10.102.189.153:1025'}
[Mon26 14:32] - failures.py       :34  :j.<module>           - INFO     - check d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4 status
[Mon26 14:32] - failures.py       :34  :j.<module>           - INFO     - check a9781952-cefd-48e9-a29a-981a64f0e717 on node ac1f6b4571d8 status
[Mon26 14:32] - failures.py       :36  :j.<module>           - INFO     - status : ok
[Mon26 14:32] - failures.py       :36  :j.<module>           - INFO     - status : ok
[Mon26 14:32] - failures.py       :34  :j.<module>           - INFO     - check a14ced03-451f-4564-a17d-f90feabdec37 on node ac1f6b4571d0 status
[Mon26 14:32] - failures.py       :36  :j.<module>           - INFO     - status : ok
[Mon26 14:32] - failures.py       :284 :j.<module>           - INFO     - check status tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on node ac1f6b272370
[Mon26 14:32] - failures.py       :286 :j.<module>           - INFO     - status : running ok
test001_zrobot_kill ... [Mon26 14:32] - s3.py             :43  :j.s3demo             - INFO     - get s3 client : ParseResult(scheme='http', netloc='172.27.24.226:1024', path='', params='', query='', fragment='')
[Mon26 14:32] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:32] - s3.py             :172 :j.s3demo             - INFO     - bucket : 0a6onpdw4u97wf98
[Mon26 14:32] - s3.py             :190 :j.s3demo             - INFO     - upload 6o615l1cw9a8kx3d file to 0a6onpdw4u97wf98 bucket
[Mon26 14:32] - test03_zrobot.py  :15  :j.s3_failures        - INFO     - kill zrobot process and make sure it will restart automatically
[Mon26 14:32] - test03_zrobot.py  :17  :j.s3_failures        - INFO     - minio node adder : 172.27.24.226
[Mon26 14:32] - failures.py       :383 :j.<module>           - INFO     - kill the robot on node172.27.24.226
[Mon26 14:32] - failures.py       :390 :j.<module>           - INFO     - the robot process has been killed
[Mon26 14:32] - failures.py       :392 :j.<module>           - INFO     - wait for the robot to restart
[Mon26 14:32] - failures.py       :399 :j.<module>           - INFO     - zrobot took 0.7511742115020752 sec to restart
[Mon26 14:32] - test03_zrobot.py  :21  :j.s3_failures        - INFO     - Download uploaded file, and check that both are same.
[Mon26 14:32] - s3.py             :202 :j.s3demo             - INFO     - download 6o615l1cw9a8kx3d file form 0a6onpdw4u97wf98 bucket
passed
- kill minio process and make sure it will restart automatically. ... [Mon26 14:32] - test03_zrobot.py  :29  :j.s3_failures        - INFO     - kill minio process and make sure it will restart automatically
[Mon26 14:32] - failures.py       :74  :j.<module>           - INFO     - killing minio process
[Mon26 14:32] - failures.py       :77  :j.<module>           - INFO     - minio process killed
[Mon26 14:32] - failures.py       :79  :j.<module>           - INFO     - wait for minio to restart
[Mon26 14:32] - failures.py       :86  :j.<module>           - INFO     - minio took 10.197133779525757 sec to restart
passed
[Mon26 14:32] - base_test.py      :89  :j.s3_failures        - INFO     - s3_demo_5 url : {'public': 'http://172.27.24.226:1024', 'storage': 'http://10.102.189.153:1025'}
[Mon26 14:32] - failures.py       :34  :j.<module>           - INFO     - check a9781952-cefd-48e9-a29a-981a64f0e717 on node ac1f6b4571d8 status
[Mon26 14:32] - failures.py       :34  :j.<module>           - INFO     - check a14ced03-451f-4564-a17d-f90feabdec37 on node ac1f6b4571d0 status
[Mon26 14:32] - failures.py       :36  :j.<module>           - INFO     - status : ok
[Mon26 14:32] - failures.py       :36  :j.<module>           - INFO     - status : ok
[Mon26 14:32] - failures.py       :34  :j.<module>           - INFO     - check d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4 status
[Mon26 14:32] - failures.py       :36  :j.<module>           - INFO     - status : ok
[Mon26 14:33] - failures.py       :284 :j.<module>           - INFO     - check status tlog zdb ac361150-1ebc-4bb7-8e2b-de15d0cdae8e on node ac1f6b272370
[Mon26 14:33] - failures.py       :286 :j.<module>           - INFO     - status : running ok
test001_bitrot ... [Mon26 14:33] - test05_bitrot.py  :12  :j.s3_failures        - INFO     - Make sure all zdbs are up
[Mon26 14:33] - test05_bitrot.py  :56  :j.s3_failures        - INFO     - Get the minio namespaces and get the zdbs location
[Mon26 14:33] - test05_bitrot.py  :60  :j.s3_failures        - INFO     - Get the extra 25% namespaces down
[Mon26 14:33] - failures.py       :155 :j.<module>           - INFO     - stop d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4
[Mon26 14:33] - test05_bitrot.py  :66  :j.s3_failures        - INFO     - Upload a file and get its md5sum, should succeed.
[Mon26 14:33] - s3.py             :43  :j.s3demo             - INFO     - get s3 client : ParseResult(scheme='http', netloc='172.27.24.226:1024', path='', params='', query='', fragment='')
[Mon26 14:33] - s3.py             :171 :j.s3demo             - INFO     - create bucket
[Mon26 14:33] - s3.py             :172 :j.s3demo             - INFO     - bucket : g53b14be5dty5l7u
[Mon26 14:33] - s3.py             :190 :j.s3demo             - INFO     - upload qxcidzp2b27oo0jm file to g53b14be5dty5l7u bucket
[Mon26 14:33] - test05_bitrot.py  :69  :j.s3_failures        - INFO     - Corrupt all data in any zdb location
[Mon26 14:33] - test05_bitrot.py  :74  :j.s3_failures        - INFO     - Get parity zdbs down excluding the corrupted namespace
[Mon26 14:33] - failures.py       :155 :j.<module>           - INFO     - stop a14ced03-451f-4564-a17d-f90feabdec37 on node ac1f6b4571d0
[Mon26 14:33] - test05_bitrot.py  :78  :j.s3_failures        - INFO     - Try to download F1, should give wrong md5
[Mon26 14:33] - s3.py             :202 :j.s3demo             - INFO     - download qxcidzp2b27oo0jm file form g53b14be5dty5l7u bucket
[Mon26 14:34] - test05_bitrot.py  :82  :j.s3_failures        - INFO     - Get parity zdbs up exculding extra_namespaces zdbs
[Mon26 14:35] - failures.py       :185 :j.<module>           - INFO     - start a14ced03-451f-4564-a17d-f90feabdec37 on node ac1f6b4571d0
[Mon26 14:35] - test05_bitrot.py  :85  :j.s3_failures        - INFO     - Run the bitrot protection, should succeed
[Mon26 14:37] - test05_bitrot.py  :17  :j.s3_failures        - INFO     - Make sure all zdbs are up
[Mon26 14:37] - failures.py       :185 :j.<module>           - INFO     - start d5417b47-c244-4037-9641-9ce0e173e128 on node ac1f6b4573d4
ERROR
======================================================================
1) ERROR: test001_bitrot
----------------------------------------------------------------------
   Traceback (most recent call last):
    testcases/s3/test05_bitrot.py line 86 in test001_bitrot
      self.s3.minio_container.system('minio gateway zerostor-repair --config-dir /bin').get()
    /opt/code/github/threefoldtech/jumpscale_lib/JumpscaleLib/clients/zero_os_protocol/Response.py line 271 in get
      raise TimeoutError()


-----------------------------------------------------------------------------
12 tests run in 2078.770 seconds. 
1 error (11 tests passed)

```
