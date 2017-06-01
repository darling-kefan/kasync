package kasync_test

import (
	"fmt"
	"kasync"
)

func ExampleNewKaMySQLDB() {
	kaConf := kasync.MySQLConfig{
		Username: "root",
		Password: "123456",
		Host: "127.0.0.1",
		Port: 3306,
	}
	kaConn, err := kasync.NewKaMySQLDB(kaConf)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%#v", kaConn)
}
