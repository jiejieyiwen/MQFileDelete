package TaskDispatch

import (
	"DeleteFromLocal/MqModular"
	"github.com/sirupsen/logrus"
	"iPublic/AMQPModular"
	"sync"
)

type FailedRecord struct {
	ChannelID string `json:"ID" bson:"ID"`
	Table     string `json:"Table" bson:"Table"`
}

type DeleteTask struct {
	logger *logrus.Entry

	m_pMQConn  *AMQPModular.RabbServer //MQ连接
	MQCon      MqModular.ConnPool
	m_strMQURL string //MQ连接地址

	MountPointMQList     map[string][]StreamResData
	MountPointMQListLock sync.Mutex

	chDeleteFaildFile chan StreamResData

	MountPointMQList1     map[int][]StreamResData
	MountPointMQListLock1 sync.Mutex
	Index                 int
	Nummap                map[string]string

	FailedID     []FailedRecord
	FailedIDLock sync.Mutex
}

var task DeleteTask

func GetTaskManager() *DeleteTask {
	return &task
}

func (manager *DeleteTask) GetMQList(mp string) []StreamResData {
	manager.MountPointMQListLock.Lock()
	defer manager.MountPointMQListLock.Unlock()
	a := manager.MountPointMQList[mp]
	manager.MountPointMQList[mp] = []StreamResData{}
	return a
}

func (manager *DeleteTask) GetMQList1(index int) []StreamResData {
	manager.MountPointMQListLock1.Lock()
	defer manager.MountPointMQListLock1.Unlock()
	a := manager.MountPointMQList1[index]
	manager.MountPointMQList1[index] = []StreamResData{}
	return a
}
