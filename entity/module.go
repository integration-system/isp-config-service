package entity

import "time"

type Module struct {
	tableName          string    `sql:"?db_schema.modules" json:"-"`
	Id                 string    `json:"id"`
	Name               string    `json:"name" valid:"required~Required"`
	CreatedAt          time.Time `json:"createdAt" sql:",null"`
	LastConnectedAt    time.Time `json:"lastConnectedAt"`
	LastDisconnectedAt time.Time `json:"lastDisconnectedAt"`
}
