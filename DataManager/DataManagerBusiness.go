package DataManager

import (
	"StorageMaintainer1/Redis"
	"encoding/json"
	"errors"
	"fmt"
	"iPublic/DataFactory/DataDefine"
)

func (pThis *DataManager) GetStorageSchemeInfoByStorageSchemeID(strStorageSchemeID string) (DataDefine.StorageSchemeInfo, error) {
	tReturn := DataDefine.StorageSchemeInfo{}
	rec := Redis.GetRedisRecordManager()
	strPrefix := "DC_StorageSchemeInfo:Data"
	pStringCmd := rec.Srv.Client.HGet(strPrefix, strStorageSchemeID)
	if nil != pStringCmd.Err() {
		return tReturn, errors.New(fmt.Sprintf("Can`t Find StorageSchemeInfo:[%s] StorageSchemeID !", strStorageSchemeID))
	}
	strData, _ := pStringCmd.Result()
	json.Unmarshal([]byte(strData), &tReturn)
	return tReturn, nil
}

func (pThis *DataManager) GetChannelStorageInfoByID(ChannelID string) DataDefine.ChannelStorageInfo {
	tReturn := DataDefine.ChannelStorageInfo{}
	if len(ChannelID) == 0 {
		return tReturn
	} else {
		for _, v := range pThis.SliceChannelStorageInfo {
			if v.ChannelID == ChannelID {
				tReturn = v
				return tReturn
			}
		}
		return tReturn
	}
}