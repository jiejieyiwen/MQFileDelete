package main

import (
	"Config"
	"MQFileDelete/MongoDB"
	"MQFileDelete/TaskDispatch"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
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
