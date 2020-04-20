package MongoDB

import (
	"Config"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"iPublic/LoggerModular"
	"iPublic/MongoModular"
	"time"
)

type RecordFileMongo struct {
	Logger *logrus.Entry
	Srv    MongoModular.MongoDBServ
}

var recordManager RecordFileMongo

func GetMongoRecordManager() *RecordFileMongo {
	return &recordManager
}

func init() {
	recordManager.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
}

func (record *RecordFileMongo) Init() error {
	logger := LoggerModular.GetLogger()
	MongoDBURL := Config.GetConfig().MongoDBConfig.MongoDBURLMongo
	if err := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &record.Srv); err != nil {
		logger.Errorf("Init Mongo Connect Err: [%v]. ", err)
		return err
	} else {
		logger.Infof("Init Mongo Connect over url: [%v] ", MongoDBURL)
	}
	return nil
}

func SetInfoMongoToDelete(id, mp string, stime int64, srv MongoModular.MongoDBServ) (info *mgo.ChangeInfo, err error) {
	baseFilter := []interface{}{bson.M{"ChannelInfoID": id}}
	baseFilter = append(baseFilter, bson.M{"MountPoint": mp})
	tNow := time.Unix(stime, 0)
	eTime := time.Date(tNow.Year(), tNow.Month(), tNow.Day(), 0, 1440, 0, 0, time.Local).Unix()
	baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$lt": eTime}})
	//baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$gte": stime}})
	filter := bson.M{"$and": baseFilter}
	apply := mgo.Change{
		Update: bson.M{
			"LockStatus": 2,                  //设置为删除
			"DeleteTime": time.Now().Unix()}, //设置个预备删除时间，经过一段时间后，再删除MongoDB数据, 方便前期排错
		ReturnNew: true,
		Upsert:    false,
	}
	var Result interface{}
	return srv.Update1(DefaultMongoTable, filter, apply, Result)
}

func (record *RecordFileMongo) DeleteMongoTsAll(id, mp string, stime int64) (info *mgo.ChangeInfo, err error) {
	baseFilter := []interface{}{bson.M{"ChannelInfoID": id}}
	baseFilter = append(baseFilter, bson.M{"MountPoint": mp})
	tNow := time.Unix(stime, 0)
	eTime := time.Date(tNow.Year(), tNow.Month(), tNow.Day(), 0, 1440, 0, 0, time.Local).Unix()
	baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$lt": eTime}})
	//baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$gte": stime}})
	filter := bson.M{"$and": baseFilter}
	return record.Srv.DeleteAll(DefaultMongoTable, filter)
}
