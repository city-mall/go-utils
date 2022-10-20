package redis

import (
	"errors"
	"fmt"

	"github.com/go-redis/redis/v8"
	zerolog "github.com/rs/zerolog/log"
)

type redisClient struct {
	client *redis.Client
}

type RedisConfig struct {
	Name string
	Host string
	Port int
}

var rdb = make(map[string]*redisClient)

func Connect(config RedisConfig) {
	opt, err := redis.ParseURL(config.Host + ":" + fmt.Sprint(config.Port))
	if err != nil {
		zerolog.Error().Msgf("Error in parsing redis URL")
		return
	}
	rdb[config.Name] = &redisClient{
		client: nil,
	}
	rdb[config.Name].client = redis.NewClient(opt)
	zerolog.Log().Msgf("Redis %v connected", config.Name)
}

func GetRedis(name string) (*redis.Client, error) {
	if rdCli, found := rdb[name]; found {
		return rdCli.client, nil
	} else {
		e := "Redis Client " + name + " not found"
		return nil, errors.New(e)
	}
}
