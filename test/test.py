import redis 

redis_client1 = redis.Redis(host='skyfire-broadside-08.group-megvii-transformer-hound.megvii-transformer.svc', port=6379)
redis_client2 = redis.Redis(host='broadside-new-ait.group-megvii-transformer-hound.megvii-transformer.svc', port=6379)
redis_client3 = redis.Redis(host='localhost', port=6379)

keys1 = redis_client1.keys('*')
keys2 = redis_client2.keys('*')
keys3 = redis_client3.keys('*')

print("测试环境redis: ", keys1)
print("生产环境redis: ", keys2)
print("本地容器redis: ", keys3)