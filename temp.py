import orjson
import redis
#
REDIS_HOST = '47.103.187.110'
REDIS_PORT = 6379
REDIS_PASS = 'Ms123456'
#
#
server_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS)
#
list_b_md = server_redis.mget('market_000001.SZ', 'market_000002.SZ', 'market_511990.SH')
#
#
# # dict_secid2lastpx = {}
# # for dict_md in list_dicts_md:
# #     secid = dict_md['HTSCSecurityID']
# #     lastpx = dict_md['LastPx']
# #     dict_secid2lastpx.update({secid, lastpx})
# a = server_redis.get('market_688260.SH')
# b_md = orjson.loads(a)['LastPx'] / 10000
# print(b_md)

a = server_redis.get('index_000016.SH')
b_md = orjson.loads(a)['LastPx'] / 10000
print(b_md)


