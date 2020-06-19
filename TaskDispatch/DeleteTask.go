package TaskDispatch

import (
	"Config"
	"MQFileDelete/MongoDB"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"iPublic/AMQPModular"
	"iPublic/LoggerModular"
	"iPublic/MongoModular"
	"math/rand"
	"strings"
	"time"
)

func (manager *DeleteTask) Init() error {
	rand.Seed(time.Now().UnixNano())
	manager.logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	manager.Index = 0

	manager.m_strMQURL = Config.GetConfig().PublicConfig.AMQPURL

	//size := 1
	//manager.MQCon.Init(size, manager.m_strMQURL)
	//manager.logger.Infof("Init MQ Success: [%v]", manager.m_strMQURL)

	manager.m_pMQConn = new(AMQPModular.RabbServer)
	err := AMQPModular.GetRabbitMQServ(manager.m_strMQURL, manager.m_pMQConn)
	if err != nil {
		manager.logger.Errorf("Init MQ Failed, Errors: %v", err.Error())
		return err
	} else {
		manager.logger.Infof("Init MQ Success: [%v]", manager.m_strMQURL)
	}

	manager.MountPointMQList = make(map[string][]StreamResData)
	manager.MountPointMQList1 = make(map[int][]StreamResData)
	manager.Nummap = make(map[string]string)
	manager.Nummap["01"] = "1"
	manager.Nummap["02"] = "2"
	manager.Nummap["03"] = "3"
	manager.Nummap["04"] = "4"
	manager.Nummap["05"] = "5"
	manager.Nummap["06"] = "6"
	manager.Nummap["07"] = "7"
	manager.Nummap["08"] = "8"
	manager.Nummap["09"] = "9"

	go manager.goGetMQMsg()

	srv := MongoDB.GetMongoRecordManager()
	srv1 := MongoDB.GetMongoRecordManager1()
	for i := 0; i < MongoDB.ConNUm; i++ {
		go manager.goGetResultsByMountPoint2(i, srv.Srv[i], srv1.Srv[i])
	}
	return nil
}

func (manager *DeleteTask) goDetletMongoFileAll2(result StreamResData, mp string, srv MongoModular.MongoDBServ, index int) {
	logger := LoggerModular.GetLogger()
	switch result.GetNRespond() {
	case 1:
		{
			logger.Infof("服务器文件夹删除成功：[%v], 协程: [%v]", result, mp)
			//if strings.Contains(result.StrMountPoint, "yyxs") {
			//	return
			//}
			date := strings.Split(result.StrDate, "-")
			v := manager.Nummap[date[2]]
			if v != "" {
				date[2] = v
			}
			date1 := date[0] + date[1] + date[2]
			t1 := time.Now()
			if data, err, table := MongoDB.GetMongoRecordManager().DeleteMongoTsAll(result.GetStrChannelID(), srv, date[2], date1); err != nil {
				t2 := time.Now()
				logger.Errorf("首次删除mongo记录失败, ChannelID[%s], result.StrMountPoint: [%v], Error: [%v], 耗时: [%v], Table: [%v], 协程: [%v], Date: [%v]", result.StrChannelID, result.StrMountPoint, err, t2.Sub(t1).Seconds(), table, index, date1)
				time.Sleep(time.Second * 3)
				for {
					t11 := time.Now()
					if data, err1, table := MongoDB.GetMongoRecordManager().DeleteMongoTsAll(result.GetStrChannelID(), srv, date[2], date1); err != nil {
						t2 := time.Now()
						logger.Errorf("重试删除mongo记录失败, ChannelID[%s], result.StrMountPoint: [%v], Error: [%v], 耗时: [%v], Table: [%v], 协程: [%v], Date: [%v]", result.StrChannelID, result.StrMountPoint, err1, t2.Sub(t11).Seconds(), table, index, date1)
						time.Sleep(time.Second * 3)
						continue
					} else {
						t2 := time.Now()
						logger.Infof("再次删除mongo记录成功: [%v], 文件数：[%v], 耗时: [%v], 协程: [%v], Table: [%v], 协程: [%v], Date: [%v]", result, data.Removed, t2.Sub(t11).Seconds(), mp, table, index, date1)
						break
					}
				}
				return
			} else {
				t2 := time.Now()
				logger.Infof("首次删除mongo记录成功: [%v], 文件数：[%v], 耗时: [%v], 协程: [%v], Table: [%v], 协程: [%v], Date: [%v]", result, data.Removed, t2.Sub(t1).Seconds(), mp, table, index, date1)
				return
			}
		}
	case -1:
		{
			logger.Errorf("删除文件失败, 信息错误: [%v], 协程: [%v]", result, index)
			return
		}
	case -2:
		{
			logger.Errorf("删除文件失败, 脚本删除不成功: [%v], 协程: [%v]", result, index)
			return
		}
	}
}

func (manager *DeleteTask) goGetResultsByMountPoint2(index int, srv MongoModular.MongoDBServ, srv1 MongoModular.MongoDBServ) {
	manager.logger.Infof("MQ消息处理协程开始工作: [%v]", index)
	for {
		chmq := manager.GetMQList1(index)
		for _, result := range chmq {
			if result.NType == 4 {
				manager.goDetletMongoFileAll2(result, result.StrMountPoint, srv1, index)
			} else if result.NType == 0 || result.NType == 9 {
				manager.goDetletMongoFileAll2(result, result.StrMountPoint, srv1, index)
			}
			time.Sleep(time.Nanosecond)
		}
		time.Sleep(time.Millisecond * 5)
	}
}

func (manager *DeleteTask) goGetMQMsg() {
	for {
		queue, err := manager.m_pMQConn.QueueDeclare("RecordDelete", false, false)
		if err != nil {
			manager.logger.Errorf("QueueDeclare Error: %s", err) // 声明队列, 设置为排他队列，链接断开后自动关闭删除
			return
		}
		err = manager.m_pMQConn.AddConsumer("test", queue) //添加消费者
		if err != nil {
			manager.logger.Errorf("AddConsumer Error: %s", err) // 声明队列, 设置为排他队列，链接断开后自动关闭删除
			return
		}
		//只能有一个消费者
		for _, delivery := range queue.Consumes {
			manager.logger.Infof("MQ Consumer: %s", "test")
			manager.m_pMQConn.HandleMessage(delivery, manager.HandleMessage1)
		}
		//重连
		if <-manager.m_pMQConn.Break {
			manager.logger.Error("MQ Disconnect")
			manager.m_pMQConn = new(AMQPModular.RabbServer)
			err := AMQPModular.GetRabbitMQServ(manager.m_strMQURL, manager.m_pMQConn)
			if err != nil {
				manager.logger.Errorf("ReInit MQ Failed, Errors: %v", err.Error())
			} else {
				manager.logger.Infof("ReInit MQ Success: [%v]", manager.m_strMQURL)
			}
		}
	}
}

func (manager *DeleteTask) HandleMessage1(data []byte) error {
	var msgBody StreamResData
	err := json.Unmarshal(data, &msgBody)
	if nil == err {
		manager.MountPointMQListLock1.Lock()
		manager.MountPointMQList1[manager.Index] = append(manager.MountPointMQList1[manager.Index], msgBody)
		manager.Index++
		if manager.Index >= MongoDB.ConNUm {
			manager.Index = 0
		}
		manager.MountPointMQListLock1.Unlock()
		return nil
	}
	manager.logger.Errorf("Received Error: [%v]", err)
	return err
}

func (manager *DeleteTask) DeleteFailMongoRecord() {
	logger := LoggerModular.GetLogger()
	srv := MongoDB.GetMongoRecordManager()
	manager.FailedIDLock.Lock()
	for _, v := range manager.FailedID {
		data, err, table := MongoDB.GetMongoRecordManager().DeleteFailFileOnMongo(v.ChannelID, v.Table, srv.Srv[0])
		if err != nil {
			logger.Errorf("Delete MongoFailFile Error", err)
			count := 0
			time.Sleep(time.Second * 3)
			for {
				data, err, table := MongoDB.GetMongoRecordManager().DeleteFailFileOnMongo(v.ChannelID, v.Table, srv.Srv[0])
				if err != nil {
					count++
					if count >= 3 {
						break
					} else {
						time.Sleep(time.Second * 3)
						continue
					}
				} else {
					manager.logger.Infof("ReDelete MongoFailFile Success, ID: [%v], FileCount: [%v], Table: [%v]", v.ChannelID, data.Removed, table)
					_, err1 := MongoDB.GetMongoCOnManager().DeleteRecordOnMongo(v.ChannelID)
					if err1 != nil {
						manager.logger.Errorf("Delete DeleteMongoFailFile Record Error", err)
					} else {
						manager.logger.Infof("Delete DeleteMongoFailFile Success, ID: [%v], FileCount: [%v]", v.ChannelID, data.Removed)
					}
					break
				}
			}
		} else {
			manager.logger.Infof("Delete MongoFailFile Success, ID: [%v], FileCount: [%v], Table: [%v]", v.ChannelID, data.Removed, table)
			_, err1 := MongoDB.GetMongoCOnManager().DeleteRecordOnMongo(v.ChannelID)
			if err1 != nil {
				manager.logger.Errorf("Delete DeleteMongoFailFile Record Error", err)
			} else {
				manager.logger.Infof("Delete DeleteMongoFailFile Success, ID: [%v], FileCount: [%v]", v.ChannelID, data.Removed)
			}
		}
	}
	manager.FailedID = []FailedRecord{}
	manager.FailedIDLock.Unlock()
}
