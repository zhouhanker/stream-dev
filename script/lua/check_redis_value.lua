-- redis client -> EVAL "return redis.call('SISMEMBER', KEYS[1], ARGV[1])" 1 sensitive_words 气枪价格
return redis.call('SISMEMBER', KEYS[1], ARGV[1])
