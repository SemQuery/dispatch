package common

import (
    "gopkg.in/redis.v3"
)

var Redis *redis.Client

func InitRedis() {
    Redis = redis.NewClient(&redis.Options {
        Addr: Config.RedisAddr,
        Password: Config.RedisPass,
        DB: Config.RedisDB,
    })
}
