package kasync_test

import (
	"time"
	"testing"

	"kasync"
)


var conf = kasync.MySQLConfig{
	//Username: "root",
	//Password: "123456",
	//Host: "localhost",
	//Port: 3306,

	Username: "root",
	Password: "tvmining@123",
	Host: "10.10.72.64",
	Port: 3306,
}

func TestFind(t *testing.T) {
	kaDb, err := kasync.NewKaMySQLDB(conf)
	if err != nil {
		t.Errorf("%v", err)
	}
	defer kaDb.Close()

	publish, err := kaDb.Find(430)
	if err != nil {
		t.Errorf("%v", err)
	}

	t.Logf("%#v", publish)

	t.Logf("%#v", publish.Stime.String)
}

func TestListRunningPubs(t *testing.T) {
	kaDb, err := kasync.NewKaMySQLDB(conf)
	if err != nil {
		t.Errorf("%v", err)
	}
	defer kaDb.Close()

	publishs, err := kaDb.ListRunningPubs(0, 0)
	if err != nil {
		t.Errorf("%v", err)
	}

	for _, p := range publishs {
		t.Logf("%#v", p)
	}
}

func TestListPubsByDate(t *testing.T) {
	kaDb, err := kasync.NewKaMySQLDB(conf)
	if err != nil {
		t.Errorf("%v", err)
	}
	defer kaDb.Close()

	publishs, err := kaDb.ListPubsByDate(time.Now().AddDate(0, -5, 0), 0, 0)
	if err != nil {
		t.Errorf("%v", err)
	}

	for _, p := range publishs {
		t.Logf("%#v", p)
	}
	t.Logf("%#v", time.Now().AddDate(0, -1, 0).Format("2006-01-02 15:04:05"))
}

func TestListPubsByUpdatedDuration(t *testing.T) {
	kaDb, err := kasync.NewKaMySQLDB(conf)
	if err != nil {
		t.Errorf("%v", err)
	}
	defer kaDb.Close()

	publishs, err := kaDb.ListPubsByUpdatedDuration(time.Now().AddDate(-1, 0, 0), time.Time{}, 0, 10)
	if err != nil {
		t.Errorf("%v", err)
	}

	for _, p := range publishs {
		t.Logf("%#v", p)
	}
}

func TestGetPubver(t *testing.T) {
	kaDb, err := kasync.NewKaMySQLDB(conf)
	if err != nil {
		t.Errorf("%v", err)
	}
	defer kaDb.Close()

	pubversion, err := kaDb.GetPubver(655)
	if err != nil {
		t.Errorf("%v", err)
	}

	t.Logf("%#v", pubversion)
}

func TestGetLatestPubver(t *testing.T) {
	kaDb, err := kasync.NewKaMySQLDB(conf)
	if err != nil {
		t.Errorf("%v", err)
	}
	defer kaDb.Close()

	pubversion, err := kaDb.GetLatestPubver(430)
	if err != nil {
		t.Errorf("%v", err)
	}

	t.Logf("%#v", pubversion)
}

func TestGetPubverByVersion(t *testing.T) {
	kaDb, err := kasync.NewKaMySQLDB(conf)
	if err != nil {
		t.Errorf("%v", err)
	}
	defer kaDb.Close()

	pubversion, err := kaDb.GetPubverByVersion("66da83c0bd341406")
	if err != nil {
		t.Errorf("%v", err)
	}

	t.Logf("%#v", pubversion)
}

// @TODO 返回值有问题
func TestListLatestIdsByPubids(t *testing.T) {
	kaDb, err := kasync.NewKaMySQLDB(conf)
	if err != nil {
		t.Errorf("%v", err)
	}
	defer kaDb.Close()

	ids, err := kaDb.ListLatestIdsByPubids([]int64{641,642,643,644,645})
	if err != nil {
		t.Errorf("%v", err)
	}

	t.Logf("%#v", ids)
}
