package TaskDispatch

import (
	AMQPModular "AMQPModular2"
	"Config"
	"MQFileDelete/MongoDB"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
	"iPublic/LoggerModular"
	"math/rand"
	"time"
)

var ConcurrentNumber int

func (manager *DeleteTask) Init() error {
	rand.Seed(time.Now().UnixNano())
	manager.logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	manager.m_pMQConn = new(AMQPModular.RabbServer)
	manager.m_strMQURL = Config.GetConfig().PublicConfig.AMQPURL
	//manager.m_strMQURL = "amqp://guest:guest@192.168.0.56:30001/"
	//manager.m_strMQURL = "amqp://dengyw:dengyw@49.234.88.77:5672/dengyw"
	err := AMQPModular.GetRabbitMQServ(manager.m_strMQURL, manager.m_pMQConn)
	if err != nil {
		manager.logger.Errorf("Init MQ Failed, Errors: %v", err.Error())
		return err
	} else {
		manager.logger.Infof("Init MQ Success: [%v]", manager.m_strMQURL)
	}
	manager.MountPointMQList = make(map[string][]StreamResData)
	go manager.goGetMQMsg()
	return nil
}

func (manager *DeleteTask) goDetletMongoFile(result StreamResData, mp string, chxianxiu chan int) {
	if !bson.IsObjectIdHex(result.GetStrRecordID()) {
		manager.logger.Error("收到的信息错误")
		<-chxianxiu
		return
	}
	switch result.GetNRespond() {
	case 1:
		{
			manager.logger.Infof("文件删除成功：[%v], 协程: [%v]", result, mp)
			t1 := time.Now()
			if data, err := MongoDB.GetMongoRecordManager().DeleteMongoTsAll(result.StrChannelID, result.StrMountPoint, result.NStartTime); err != nil {
				t2 := time.Now()
				manager.logger.Errorf("删除mongo记录失败, ChannelID[%s], result.StrMountPoint: [%v], NStartTime: [%v], Error: [%v], 耗时: [%v]", result.StrChannelID, result.StrMountPoint, result.NStartTime, err, t2.Sub(t1).Seconds())
				<-chxianxiu
				return
			} else {
				t2 := time.Now()
				manager.logger.Infof("删除mongo记录成功: [%v], 文件数：[%v], 耗时: [%v], 协程: [%v]", result, data.Removed, t2.Sub(t1).Seconds(), mp)
				<-chxianxiu
				return
			}
		}
	case -1:
		{
			manager.logger.Infof("删除文件失败, 信息错误: [%v], 协程: [%v]", result, mp)
			<-chxianxiu
			return
		}
	case -2:
		{
			manager.logger.Infof("删除文件失败, 脚本删除不成功: [%v], 协程: [%v]", result, mp)
			<-chxianxiu
			return
		}
	case 3:
		{
			manager.logger.Infof("文件夹已删除: [%v], 协程: [%v]", result, mp)
			t1 := time.Now()
			if data, err := MongoDB.GetMongoRecordManager().DeleteMongoTsAll(result.StrChannelID, result.StrMountPoint, result.NStartTime); err != nil {
				t2 := time.Now()
				manager.logger.Errorf("删除mongo记录失败, ChannelID[%s], result.StrMountPoint: [%v], NStartTime: [%v], Error: [%v], 耗时: [%v]", result.StrChannelID, result.StrMountPoint, result.NStartTime, err, t2.Sub(t1).Seconds())
				<-chxianxiu
				return
			} else {
				t2 := time.Now()
				manager.logger.Infof("删除mongo记录成功: [%v], 文件数：[%v], 耗时: [%v], 协程: [%v]", result, data.Removed, t2.Sub(t1).Seconds(), mp)
				<-chxianxiu
				return
			}
		}
	}
}

func (manager *DeleteTask) goGetResultsByMountPoint(mp string) {
	manager.logger.Infof("MQ消息处理协程开始工作: [%v]", mp)
	chxianliu := make(chan int, 100)
	for {
		chmq := manager.GetMQList(mp)
		for key, result := range chmq {
			chxianliu <- key
			go manager.goDetletMongoFile(result, mp, chxianliu)
		}
		time.Sleep(time.Millisecond)
	}
}

func (manager *DeleteTask) goGetMQMsg() {
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
}

func (manager *DeleteTask) HandleMessage1(data []byte) error {
	var msgBody StreamResData
	err := json.Unmarshal(data, &msgBody)
	if nil == err {
		manager.logger.Infof("Received a message: [%v]", msgBody)
		manager.MountPointMQListLock.Lock()
		if _, ok := manager.MountPointMQList[msgBody.StrMountPoint]; !ok {
			manager.MountPointMQList[msgBody.StrMountPoint] = append(manager.MountPointMQList[msgBody.StrMountPoint], msgBody)
			go manager.goGetResultsByMountPoint(msgBody.StrMountPoint)
		} else {
			manager.MountPointMQList[msgBody.StrMountPoint] = append(manager.MountPointMQList[msgBody.StrMountPoint], msgBody)
		}
		manager.MountPointMQListLock.Unlock()
		return nil
	}
	manager.logger.Errorf("Received Error: [%v]", err)
	return err
}
