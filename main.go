package main

import (
	"Config"
	"MQFileDelete/MongoDB"
	"MQFileDelete/TaskDispatch"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"iPublic/RedisModular"
	"os"
	"strconv"
)

func init() {
	EnvLoad.GetCmdLineConfig()
}

func main() {
	logger := LoggerModular.GetLogger()

	config := Config.GetConfig()
	if err := Config.ReadConfig(); err != nil {
		logger.Error(err)
		return
	}
	logger.Infof("Config is [%v]", config)

	if len(os.Args) > 1 {
		for index, k := range os.Args {
			switch k {
			case "-Con":
				{
					MongoDB.ConNUm, _ = strconv.Atoi(os.Args[index+1])
				}
			}
		}
	}

	conf := EnvLoad.GetConf()
	conf.RedisAppName = "imccp-mediacore-media-MQFileDelete"
	RedisModular.GetBusinessMap().SetBusinessRedis(EnvLoad.PublicName, Config.GetConfig().PublicConfig.RedisURL)
	EnvLoad.GetServiceManager().SetStatus(EnvLoad.ServiceStatusOK)
	go EnvLoad.GetServiceManager().RegSelf()

	//mongo
	if err := MongoDB.GetMongoRecordManager().Init(); err != nil {
		logger.Error(err)
		return
	} else {
		if err := TaskDispatch.GetTaskManager().Init(); err != nil {
			logger.Error(err)
			return
		}
	}

	a := make(chan bool)
	<-a
}
