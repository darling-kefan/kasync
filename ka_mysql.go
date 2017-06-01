package kasync

import (
	"fmt"
	"time"
	"strconv"
	"strings"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLConfig struct {
	// Optional.
	Username, Password string
	
	// Required
	Host string
	Port int
}

// dataStoreName returns a connection string suitable for sql.Open.
func (c MySQLConfig) dataStoreName(dbName string) string {
	// [username[:password]@]
	var cred string
	if c.Username != "" {
		cred = c.Username
		if c.Password != "" {
			cred = cred + ":" + c.Password
		}
		cred = cred + "@"
	}

	//return fmt.Sprintf("%stcp([%s]:%d)/%s?parseTime=true", cred, c.Host, c.Port, dbName)
	return fmt.Sprintf("%stcp([%s]:%d)/%s", cred, c.Host, c.Port, dbName)
}

type KaMySQLDB struct {
	Conn *sql.DB
}

func NewKaMySQLDB(c MySQLConfig) (*KaMySQLDB, error) {
	conn, err := sql.Open("mysql", c.dataStoreName("ka"))
	if err != nil {
		return nil, fmt.Errorf("mysql: could not get a connection: %v", err)
	}
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("mysql: could not establish a good connection %v", err)
	}

	return &KaMySQLDB{Conn: conn}, nil
}

func (kaDb *KaMySQLDB) Close() error {
	return kaDb.Conn.Close()
}

func (kaDb *KaMySQLDB) Find(id int) (*Publish, error) {
	query := "SELECT `id`,`advertiser_id`,`adposition_id`,`title`,`summary`,`stime`,`etime`,`bid`,`ceiling`,`allceiling`,`status`,`check`,`type`,`again_num`,`deleted_at`,`created_at`,`updated_at` FROM `publishs` WHERE `id` = ?"
	preStmt, err := kaDb.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer preStmt.Close()

	rows, err := preStmt.Query(id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	publishs, err := scanKaPublishs(rows)
	if err != nil {
		return nil, err
	}

	if len(publishs) == 0 {
		return nil, fmt.Errorf("The record of id %d is not existed.", id)
	}
	return publishs[0], nil
}

func (kaDb *KaMySQLDB) ListRunningPubs(offset int, limit int) ([]*Publish, error) {
	query := "SELECT `id`,`advertiser_id`,`adposition_id`,`title`,`summary`,`stime`,`etime`,`bid`,`ceiling`,`allceiling`,`status`,`check`,`type`,`again_num`,`deleted_at`,`created_at`,`updated_at` FROM `publishs` WHERE `stime` <= ? AND `etime` >= ? AND `deleted_at` is not null ORDER BY `updated_at` ASC"
	if limit > 0 {
		query = query+" LIMIT ?,?"
	}

	preStmt, err := kaDb.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	var rows *sql.Rows
	now := time.Now().Format("2006-01-02 15:04:05")
	if limit > 0 {
		rows, err = preStmt.Query(now, now, offset, limit)
	} else {
		rows, err = preStmt.Query(now, now)
	}

	publishs, err := scanKaPublishs(rows)
	if err != nil {
		return nil, err
	}

	return publishs, nil
}

func (kaDb *KaMySQLDB) ListPubsByDate(date time.Time, offset int, limit int) ([]*Publish, error) {
	query := "SELECT `id`,`advertiser_id`,`adposition_id`,`title`,`summary`,`stime`,`etime`,`bid`,`ceiling`,`allceiling`,`status`,`check`,`type`,`again_num`,`deleted_at`,`created_at`,`updated_at` FROM `publishs` WHERE `stime` <= ? AND `etime` >= ? AND `deleted_at` is not null ORDER BY `updated_at` ASC"
	if limit > 0 {
		query = query+" LIMIT ?,?"
	}

	preStmt, err := kaDb.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	var rows *sql.Rows
	dateTime := date.Format("2006-01-02 15:04:05")
	if limit > 0 {
		rows, err = preStmt.Query(dateTime, dateTime, offset, limit)
	} else {
		rows, err = preStmt.Query(dateTime, dateTime)
	}

	publishs, err := scanKaPublishs(rows)
	if err != nil {
		return nil, err
	}

	return publishs, nil
}

func (kaDb *KaMySQLDB) ListPubsByUpdatedDuration(leftBorder time.Time, rightBorder time.Time, offset int, limit int) ([]*Publish, error) {
	query := "SELECT `id`,`advertiser_id`,`adposition_id`,`title`,`summary`,`stime`,`etime`,`bid`,`ceiling`,`allceiling`,`status`,`check`,`type`,`again_num`,`deleted_at`,`created_at`,`updated_at` FROM `publishs` WHERE `updated_at` >= ? AND `updated_at` <= ? ORDER BY `updated_at` ASC"
	if limit > 0 {
		query = query+" LIMIT ?,?"
	}

	preStmt, err := kaDb.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	var leftTime, rightTime string
	if leftBorder.IsZero() {
		leftTime = time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05")
	} else {
		leftTime = leftBorder.Format("2006-01-02 15:04:05")
	}
	if rightBorder.IsZero() {
		rightTime = time.Now().Format("2006-01-02 15:04:05")
	} else {
		rightTime = rightBorder.Format("2006-01-02 15:04:05")
	}
	
	var rows *sql.Rows
	if limit > 0 {
		rows, err = preStmt.Query(leftTime, rightTime, offset, limit)
	} else {
		rows, err = preStmt.Query(leftTime, rightTime)
	}

	publishs, err := scanKaPublishs(rows)
	if err != nil {
		return nil, err
	}

	return publishs, nil
}

func scanKaPublishs(rows *sql.Rows) ([]*Publish, error) {
	var publishs []*Publish

	for rows.Next() {
		publish := new(Publish)
		err := rows.Scan(
			&publish.Id,
			&publish.AdvertiserId,
			&publish.AdpositionId,
			&publish.Title,
			&publish.Summary,
			&publish.Stime,
			&publish.Etime,
			&publish.Bid,
			&publish.Ceiling,
			&publish.Allceiling,
			&publish.Status,
			&publish.Check,
			&publish.Type,
			&publish.AgainNum,
			&publish.DeletedAt,
			&publish.CreatedAt,
			&publish.UpdatedAt,
			)
		if err != nil {
			return nil, err
		}
		publishs = append(publishs, publish)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return publishs, nil
}


func (kaDb *KaMySQLDB) GetPubver(id int64) (*Pubversion, error) {
	query := "SELECT * FROM `pubversions` WHERE `id` = ?"
	preStmt, err := kaDb.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer preStmt.Close()

	rows, err := preStmt.Query(id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pubversions, err := scanKaPubversions(rows)
	if err != nil {
		return nil, err
	}

	if len(pubversions) == 0 {
		return nil, fmt.Errorf("The pubversion of id %d is not existed.", id)
	}
	return pubversions[0], nil
}

func (kaDb *KaMySQLDB) GetLatestPubver(pubid int64) (*Pubversion, error) {
	query := "SELECT * FROM `pubversions` WHERE `publish_id` = ? ORDER BY `id` DESC LIMIT 0, 1"
	preStmt, err := kaDb.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer preStmt.Close()

	rows, err := preStmt.Query(pubid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pubversions, err := scanKaPubversions(rows)
	if err != nil {
		return nil, err
	}

	if len(pubversions) == 0 {
		return nil, fmt.Errorf("The pubversion of publish_id %d is not existed.", pubid)
	}
	return pubversions[0], nil
}

func (kaDb *KaMySQLDB) GetPubverByVersion(version string) (*Pubversion, error) {
	query := "SELECT * FROM `pubversions` WHERE `version` = ?"
	preStmt, err := kaDb.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer preStmt.Close()

	rows, err := preStmt.Query(version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pubversions, err := scanKaPubversions(rows)
	if err != nil {
		return nil, err
	}

	if len(pubversions) == 0 {
		return nil, fmt.Errorf("The pubversion of version %d is not existed.", version)
	}
	return pubversions[0], nil
}

func (kaDb *KaMySQLDB) ListLatestIdsByPubids(publishIds []int64) ([]int64, error) {
	query := "SELECT MAX(`id`) AS `id` FROM `pubversions` WHERE `publish_id` in (?) GROUP BY `publish_id`"
	preStmt, err := kaDb.Conn.Prepare(query)
	if err != nil {
		return []int64{}, nil
	}
	defer preStmt.Close()

	var pubIds []string
	for _, v := range publishIds {
		pubIds = append(pubIds, strconv.Itoa(int(v)))
	}
	if len(pubIds) == 0 {
		return []int64{}, fmt.Errorf("The param publishIds: %v is empty", publishIds)
	}
	
	rows, err := preStmt.Query(strings.Join(pubIds, ","))
	if err != nil {
		return []int64{}, err
	}
	defer rows.Close()

	fmt.Println(strings.Join(pubIds,","))
	
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return []int64{}, err
		}
		fmt.Println(id)
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return []int64{}, err
	}
	
	fmt.Println(ids)
	
	return ids, nil
}

func (kaDb *KaMySQLDB) ListPubvers(ids []int64) ([]*Pubversion, error) {
	return nil, nil
}

func (kaDb *KaMySQLDB) ListPubversByPubid(id int64) ([]*Pubversion, error) {
	return nil, nil
}

func (kaDb *KaMySQLDB) ListPubversByPubids(ids []int64) ([]*Pubversion, error) {
	return nil, nil
}

func (kaDb *KaMySQLDB) ListPubversByDatetime(ids []int64, stime time.Time, etime time.Time) ([]*Pubversion, error) {
	return nil, nil
}

func scanKaPubversions(rows *sql.Rows) ([]*Pubversion, error) {
	var pubversions []*Pubversion
	for rows.Next() {
		pubver := new(Pubversion)
		if err := rows.Scan(
			&pubver.Id,
			&pubver.AdvertiserId,
			&pubver.PublishId,
			&pubver.Version,
			&pubver.Content,
			&pubver.CreatedAt,
			&pubver.UpdatedAt,
		); err != nil {
			return nil, err
		}
		pubversions = append(pubversions, pubver)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pubversions, nil
}


type EtvmMySQLDB struct {
	Conn *sql.DB
}

func NewEtvmMySQLDB(c MySQLConfig) (*EtvmMySQLDB, error) {
	conn, err := sql.Open("mysql", c.dataStoreName("etvm"))
	if err != nil {
		return nil, fmt.Errorf("mysql: could not get a connection: %v", err)
	}
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("mysql: could not establish a good connection %v", err)
	}

	return &EtvmMySQLDB{Conn: conn}, nil
}
