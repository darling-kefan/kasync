package kasync

import (
	"time"
	"database/sql"
	_ "database/sql/driver"
)

// 表字段默认值null，则设成sql.NullXXX
type Publish struct {
	Id           int64
	AdvertiserId sql.NullInt64
	AdpositionId sql.NullInt64
	Title        sql.NullString
	Summary      sql.NullString
	Stime        sql.NullString
	Etime        sql.NullString
	Bid          sql.NullInt64
	Ceiling      sql.NullInt64
	Allceiling   sql.NullInt64
	Status       int
	Check        int
	Type         int
	AgainNum     float64
	CreatedAt    sql.NullString
	UpdatedAt    sql.NullString
	DeletedAt    sql.NullString
}

type KaPublishModel interface {
	Find(id int) (*Publish, error)
	ListRunningPubs(offset int, limit int) ([]*Publish, error)
	ListPubsByDate(date time.Time, offset int, limit int) ([]*Publish, error)
	ListPubsByUpdatedDuration(leftBorder time.Time, rightBorder time.Time, offset int, limit int) ([]*Publish, error)
}

type Pubversion struct {
	Id           int64
	AdvertiserId sql.NullInt64
	PublishId    sql.NullInt64
	Version      sql.NullString
	Content      sql.NullString
	CreatedAt    sql.NullString
	UpdatedAt    sql.NullString
}

type Pubversioner interface {
	GetPubver(id int64) (*Pubversion, error)
	GetLatestPubver(pubid int64) (*Pubversion, error)
	GetPubverByVersion(version string) (*Pubversion, error)
	ListLatestIdsByPubids(publishIds []int64) ([]int64, error)
	ListPubvers(ids []int64) ([]*Pubversion, error)
	ListPubversByPubid(id int64) ([]*Pubversion, error)
	ListPubversByPubids(ids []int64) ([]*Pubversion, error)
	ListPubversByDatetime(ids []int64, stime time.Time, etime time.Time) ([]*Pubversion, error)
}
