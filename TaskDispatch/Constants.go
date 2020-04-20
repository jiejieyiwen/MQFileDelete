package TaskDispatch

import (
	AMQPModular "AMQPModular2"
	"github.com/sirupsen/logrus"
	"sync"
)

type DeleteTask struct {
	logger               *logrus.Entry
	m_pMQConn            *AMQPModular.RabbServer //MQ连接
	m_strMQURL           string                  //MQ连接地址
	MountPointMQList     map[string][]StreamResData
	MountPointMQListLock sync.Mutex
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
