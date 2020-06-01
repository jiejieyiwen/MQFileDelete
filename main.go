package main

import (
	"Config"
	"MQFileDelete/MongoDB"
	"MQFileDelete/Redis"
	"MQFileDelete/TaskDispatch"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
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
			case "-Lim":
				{
					MongoDB.Limit, _ = strconv.Atoi(os.Args[index+1])
				}
			}
		}
	}
	//redis
	if err := Redis.GetRedisManager().Init(); err != nil {
		logger.Error(err)
		return
	}

	//mongo
	if err := MongoDB.GetMongoRecordManager().Init(); err != nil {
		logger.Error(err)
		return
	} else {
		//c := cron.New()
		//_, err := c.AddFunc("00 6 * * *", TaskDispatch.GetTaskManager().DeleteFailMongoRecord)
		//if err != nil {
		//	logger.Error(err)
		//	return
		//}
		//c.Start()
		//defer c.Stop()

		if err := TaskDispatch.GetTaskManager().Init(); err != nil {
			logger.Error(err)
			return
		}
	}

	a := make(chan bool)
	<-a
}
