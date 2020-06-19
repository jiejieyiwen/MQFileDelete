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
	Srv    []MongoModular.MongoDBServ
	Table  string
}

type MongoCon struct {
	Srv MongoModular.MongoDBServ
}

var recordManager RecordFileMongo
var recordManager1 RecordFileMongo
var mongoCon MongoCon

func GetMongoRecordManager() *RecordFileMongo {
	return &recordManager
}
func GetMongoRecordManager1() *RecordFileMongo {
	return &recordManager1
}

func GetMongoCOnManager() *MongoCon {
	return &mongoCon
}

func init() {
	recordManager.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
}

func (record *RecordFileMongo) Init() error {
	logger := LoggerModular.GetLogger()
	//动存
	MongoDBURL := Config.GetConfig().MongoDBConfig.MongoDBURLMongo
	logger.Infof("ConNUm: [%v], Limit: [%v], Date: [%v]", ConNUm, Limit)
	for i := 0; i < ConNUm; i++ {
		var srv MongoModular.MongoDBServ
		if err := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &srv); err != nil {
			logger.Errorf("Init Mongo Connect Error: [%v]", err)
			return err
		} else {
			record.Srv = append(record.Srv, srv)
			logger.Infof("Init Mongo Connect Success, Url: [%v]", MongoDBURL)
		}
	}
	//全存
	MongoDBURL = Config.GetConfig().PullStorageConfig.MongoDBURLMongo
	logger.Infof("ConNUm: [%v], Limit: [%v], Date: [%v]", ConNUm, Limit)
	for i := 0; i < ConNUm; i++ {
		var srv MongoModular.MongoDBServ
		if err := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &srv); err != nil {
			logger.Errorf("Init Pull Mongo Connect Error: [%v]", err)
			return err
		} else {
			recordManager1.Srv = append(recordManager1.Srv, srv)
			logger.Infof("Init Pull Mongo Connect Success, Url: [%v]", MongoDBURL)
		}
	}

	MongoDBURL = "mongodb://mj_ya_admin:EkJcQeOP$bGh8IYC@10.0.2.64:27017/mj_log?authSource=admin&maxPoolSize=100"
	//MongoDBURL := "mongodb://mj_ya_admin:EkJcQeOP$bGh8IYC@127.0.0.1:15678/mj_log?authSource=admin&maxPoolSize=100"
	var srv MongoModular.MongoDBServ
	if err := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &srv); err != nil {
		logger.Errorf("Init Mongo Connect Error: [%v]", err)
		return err
	} else {
		mongoCon.Srv = srv
		logger.Infof("Init Mongo Connect Success, Url: [%v]", MongoDBURL)
	}
	return nil
}

func (record *RecordFileMongo) SetInfoMongoToDelete(id, mp string, etime int64, srv MongoModular.MongoDBServ) (info *mgo.ChangeInfo, err error) {
	baseFilter := []interface{}{bson.M{"ChannelInfoID": id}}
	filter := bson.M{"$and": baseFilter}
	apply := mgo.Change{
		Update: bson.M{
			"LockStatus": 2,                  //设置为删除
			"DeleteTime": time.Now().Unix()}, //设置个预备删除时间，经过一段时间后，再删除MongoDB数据, 方便前期排错
		ReturnNew: true,
		Upsert:    false,
	}
	var Result interface{}
	return srv.Update1("RecordFileInfo", filter, apply, Result)
}

func (record *RecordFileMongo) DeleteMongoTsAll(id string, srv MongoModular.MongoDBServ, date, date1 string) (info *mgo.ChangeInfo, err error, table string) {
	baseFilter := []interface{}{bson.M{"ChannelInfoID": id}}
	baseFilter = append(baseFilter, bson.M{"Date": date1})
	filter := bson.M{"$and": baseFilter}
	Table := "RecordFileInfo_"
	Table = Table + date
	info, err = srv.DeleteAll(Table, filter)
	//ecord.Logger.Infof("Table is: [%v]", Table)
	return info, err, Table
}

func (pThis *RecordFileMongo) WriteDeleteFailFileToMongo(id, date, table string, srv MongoModular.MongoDBServ) {
	baseFilter := []interface{}{bson.M{"ID": id, "Date": date, "Table": table, "CreateTime": time.Now().Format("2006-01-02")}}
	err := srv.Insert("DeleteMongoFailFile", baseFilter)
	if err != nil {
		pThis.Logger.Errorf("Write DeleteMongoFailFile Failed", err)
		return
	}
	pThis.Logger.Info("Write DeleteMongoFailFile Success")
}

func (pThis *MongoCon) DeleteRecordOnMongo(id string) (info *mgo.ChangeInfo, err error) {
	baseFilter := []interface{}{bson.M{"ID": id}}
	filter := bson.M{"$and": baseFilter}
	return pThis.Srv.DeleteAll("DeleteMongoFailFile", filter)
}

func (pThis *RecordFileMongo) DeleteFailFileOnMongo(id, table string, srv MongoModular.MongoDBServ) (info *mgo.ChangeInfo, err error, tables string) {
	baseFilter := []interface{}{bson.M{"ChannelInfoID": id}}
	filter := bson.M{"$and": baseFilter}
	info, err = srv.DeleteAll(table, filter)
	return info, err, table
}
