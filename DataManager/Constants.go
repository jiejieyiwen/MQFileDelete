package DataManager

import (
	SDataDefine "StorageMaintainer1/DataDefine"
	"github.com/sirupsen/logrus"
	"iPublic/DataFactory"
	"iPublic/DataFactory/DataDefine"
	"sync"
)

type StorageDaysInfo struct {
	ChannelInfo string
	StorageDays int
	Path        string
}

type DataManager struct {
	/*
		存放介质，通道Token等信息
	*/
	bRunning                bool
	bRecicvedGRPCNotify     bool
	bRecicvedGRPCNotifyLock sync.RWMutex

	mpDataInterface             DataFactory.Datainterface
	SliceChannelStorageInfo     []DataDefine.ChannelStorageInfo
	SliceChannelStorageInfoLock sync.RWMutex

	TaskMap          []StorageDaysInfo
	TaskMapLock      sync.RWMutex
	NeedDeleteTsList []SDataDefine.RecordFileInfo // TS信息
	PlatformToken    string
	logger           *logrus.Logger //日志

	MountPointList     map[string][]StorageDaysInfo //挂载点表
	MountPointListLock sync.Mutex
}

var dManager DataManager
var NTSC_URL = "http://www.ntsc.ac.cn/"

//var NTSC_URL = "http://192.168.60.35:39002/imccp-mediacore"

func GetDataManager() *DataManager {
	return &dManager
}