package Redis

import (
	"StorageMaintainer1/DataDefine"
	"encoding/json"
	"github.com/sirupsen/logrus"
	ipublic "iPublic/DataFactory/DataDefine"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"iPublic/RedisModular"
	"strings"
	"sync"
)

type RecordFileRedis struct {
	Table    string                  //table name "db12"
	Srv      *RedisModular.RedisConn // RedisConnect
	Redisurl string                  //redis地址
	Logger   *logrus.Entry
}

var recordManager RecordFileRedis
var RedisSrv RedisModular.RedisConn
var RedisLib string

func GetRedisRecordManager() *RecordFileRedis {
	return &recordManager
}

func init() {
	recordManager.Table = DefaultRedisTable
	recordManager.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{"Table": DefaultRedisTable})
}

func Init() error {
	logger := LoggerModular.GetLogger()
	conf := EnvLoad.GetConf()
	recordManager.Srv = RedisModular.GetRedisPool()
	recordManager.Redisurl = conf.ServerConfig.RedisURL
	//recordManager.Redisurl = "redis://:S0o9l@7&PO@49.234.88.77:8888/7"
	//recordManager.Redisurl = "redis://:B9OxgC3HYg@192.168.0.56:30003/12"
	//recordManager.Redisurl = "redis://:inphase123.@192.168.2.64:23680/0"
	//recordManager.Redisurl = "redis://:inphase123.@127.0.0.1:15675/2"
	err := recordManager.Srv.DaliWithURL(recordManager.Redisurl)
	if err != nil {
		logger.Errorf("Init Redis  Failed，addr [%v] ,errors : %v", recordManager.Redisurl, err.Error())
		return err
	} else {
		logger.Infof("Init Redis Success!~~")
		return nil
	}
}

func (record *RecordFileRedis) GetStorageSchemeInfoFromRedis(info *[]ipublic.StorageSchemeInfo, key string, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	key = "DC_StorageSchemeInfo:Data"
	pStringStringMapCmd := record.Srv.Client.HGetAll(key)
	data, err := pStringStringMapCmd.Result()
	if err != nil {
		record.Logger.Errorf("read StorageSchemeInfo error: %v\n", err)
		return err
	} else {
		record.Logger.Infof("read StorageSchemeInfo successs")
		var ChannelList ipublic.StorageSchemeInfo
		for _, value := range data {
			err = json.Unmarshal([]byte(value), &ChannelList)
			if err != nil {
				record.Logger.Errorf(" unmarshal StorageSchemeInfo failed!~~")
				return err
			} else {
				*info = append(*info, ChannelList)
			}
		}
	}
	return nil
}

func (record *RecordFileRedis) GetStorageMediumInfoFromRedis(info *[]ipublic.StorageMediumInfo, key string, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	key = "DC_StorageMediumInfo:Data"
	pStringStringMapCmd := record.Srv.Client.HGetAll(key)
	data, err := pStringStringMapCmd.Result()
	if err != nil {
		record.Logger.Errorf("read StorageMediumInfo error: %v\n", err)
		return err
	} else {
		record.Logger.Infof("read StorageMediumInfo successs")
		var ChannelList ipublic.StorageMediumInfo
		for _, value := range data {
			err = json.Unmarshal([]byte(value), &ChannelList)
			if err != nil {
				record.Logger.Errorf(" unmarshal StorageMediumInfo failed,err:%v", err.Error())
				return err
			} else {
				*info = append(*info, ChannelList)
			}
		}
	}
	return nil
}

func (record *RecordFileRedis) GetChannelStorageInfoFromRedis(info *[]ipublic.ChannelStorageInfo, key string, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	key = "DC_ChannelStorageInfo:Data"
	pStringStringMapCmd := record.Srv.Client.HGetAll(key)
	if pStringStringMapCmd == nil {
		record.Logger.Infof("No Key Found~~!")
		return
	}
	data, err := pStringStringMapCmd.Result()
	if err != nil {
		record.Logger.Infof("Get ChannelStorageInfo from redis error: %v\n", err)
		return err
	} else {
		//ignore
		record.Logger.Infof("Get all ChannelStorageInfo from redis successs")
		var ChannelList ipublic.ChannelStorageInfo
		for _, value := range data {
			err = json.Unmarshal([]byte(value), &ChannelList)
			if err != nil {
				record.Logger.Infof(" unmarshal ChannelStorageInfo failed,err:%v", err.Error())
				return err
			} else {
				*info = append(*info, ChannelList)
			}
		}
	}
	return err
}

func (record *RecordFileRedis) GetDeleteServerConfig() (pSeverInfo map[string]string) {
	pSeverInfo = make(map[string]string)
	pStringSliceCmd := record.Srv.Client.Keys(DataDefine.KEY_NAME_SERVER_CONFIG)
	if nil != pStringSliceCmd.Err() {
		record.Logger.Error(pStringSliceCmd.Err().Error())
		return
	}

	for _, key := range pStringSliceCmd.Val() {

		strAllMountPoint, err := record.Srv.Get(key)
		if nil != err {
			continue
		}
		//地址
		arrStr := strings.Split(key, ":")
		strServerAddr := arrStr[1] + ":" + arrStr[2]

		//挂载点
		pSeverInfo[strServerAddr] = strAllMountPoint

	}
	record.Logger.Infof("Get Delete ServerSuccess ServerList[%v]", pSeverInfo)
	return
}

func (record *RecordFileRedis) DeleteServerInRedis(strKey string) error {
	pIntCmd := record.Srv.Client.HDel(DataDefine.KEY_NAME_SERVER_CONFIG, strKey)
	return pIntCmd.Err()
}

func (record *RecordFileRedis) GetMountPointFromRedis(key string) (string, error) {
	filed := "DC_StorageMediumInfo:Data"
	data, err := record.Srv.Client.HGet(filed, key).Result()
	if err != nil {
		return "", err
	}
	var datalist ipublic.StorageMediumInfo
	err = json.Unmarshal([]byte(data), &datalist)
	if err != nil {
		return "", err
	}
	return datalist.StorageMediumInfoPath, nil
}