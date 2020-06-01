package Redis

import (
	"Config"
	"github.com/sirupsen/logrus"
	"iPublic/LoggerModular"
	"iPublic/RedisModular"
)

type RedisCon struct {
	Srv      *RedisModular.RedisConn // RedisConnect
	Redisurl string                  //redis地址
	Logger   *logrus.Entry
}

var redisManager RedisCon

func GetRedisManager() *RedisCon {
	return &redisManager
}

func init() {
	redisManager.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
}

func (pThis *RedisCon) Init() error {
	pThis.Srv = RedisModular.GetRedisPool()
	pThis.Redisurl = Config.GetConfig().PublicConfig.RedisURL

	//recordManager.Redisurl = "redis://:inphase123.@127.0.0.1:15679/2"
	//recordManager.Redisurl = "redis://:inphase123.@192.168.2.64:23680/2"

	err := pThis.Srv.DaliWithURL(pThis.Redisurl)
	if err != nil {
		pThis.Logger.Errorf("Init Redis Failed, addr [%v], Error: [%v]", pThis.Redisurl, err.Error())
		return err
	} else {
		pThis.Logger.Infof("Init Redis Success: [%v]", pThis.Redisurl)
		return nil
	}
}
