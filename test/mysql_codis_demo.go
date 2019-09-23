package main

import (
	"database/sql"
	"encoding/json"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/hero5512/godis-codis"
	"github.com/hero5512/godis-codis/util"
	"log"
	"strconv"
	"time"
)

type Source struct {
	db   *sql.DB
	pool *godis_codis.RoundRobinPool
}

type UserRecord struct {
	Index   int64  `json:"index"`
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Address string `json:"address"`
}

const (
	DriverName      = "mysql"
	DataSourceName  = "root:root@tcp(localhost:3306)/test?charset=utf8"
	SearchKeyPrefix = "codis_test"
)

const (
	SelectSqlString = "select name, phone, address from user_records where id = ?"
	InsertSqlString = "insert into user_records(name, phone, address) value (?, ?, ?)"
)

func (d *Source) init() {
	pool, err := util.GetPool()
	if err != nil {
		panic(err)
	}
	d.pool = pool

	db, err := sql.Open(DriverName, DataSourceName)
	if err != nil {
		panic(err)
	}
	d.db = db
}

func (d *Source) insertUserRecord(sqlString string, user UserRecord) bool {
	stmt, err := d.db.Prepare(sqlString)
	_, err = stmt.Exec(user.Name, user.Phone, user.Address)
	if err != nil {
		log.Printf("Failed to insert user data %v", err)
		return false
	}
	return true
}

func (d *Source) selectUserRecordById(sqlString string, index int64) (user *UserRecord) {
	stmt, err := d.db.Prepare(sqlString)
	rows, err := stmt.Query(index)
	if err != nil {
		log.Printf("Failed to get %d -th user data %v", index, err)
		return nil
	}
	defer stmt.Close()
	defer rows.Close()

	for rows.Next() {
		var name, phone, address string
		if err = rows.Scan(&name, &phone, &address); err != nil {
			log.Printf("Failed to get %d -th user data %v", index, err)
		}
		user = &UserRecord{
			Index:   index,
			Name:    name,
			Phone:   phone,
			Address: address,
		}
		return user
	}
	return nil
}

func (d *Source) dbMonitor(db *sql.DB) {
	for true {
		err := db.Ping()
		if err != nil {
			println(err)
		}
		time.Sleep(5000 * time.Millisecond)
	}
}

func (d *Source) Close() {
	if d.pool != nil {
		d.pool.Close()
	}
	if d.db != nil {
		d.db.Close()
	}
}

func (d *Source) getRecord(index int64) *UserRecord {
	redisClient := d.pool.GetClient()
	searchKey := SearchKeyPrefix + strconv.FormatInt(index, 10)
	resp := redisClient.Get(searchKey)
	userRecordJson, err := resp.Result()
	if err != redis.Nil {
		println("redis result: " + userRecordJson)
		user := &UserRecord{}
		err := json.Unmarshal([]byte(userRecordJson), user)
		if err != nil {
			log.Printf("Failed to get %d -th user data %v", index, err)
			return nil
		}
		return user
	} else {
		user := d.selectUserRecordById(SelectSqlString, index)
		if user == nil {
			log.Printf("No user %d -th in database", index)
			return nil
		}
		userRecordJson, _ := json.Marshal(user)
		println("mysql result: " + string(userRecordJson))
		resp := redisClient.Set(searchKey, userRecordJson, time.Minute)
		_, err := resp.Result()
		if err != nil {
			log.Printf("Failed to set user to redis %v", err)
		}
		return user
	}

	return nil
}

func main() {
	//selectSqlString := "select name, phone, address from user_records where id = ?"
	//insertSqlString := "insert into user_records(name, phone, address) value (?, ?, ?)"
	d := Source{}
	d.init()
	//
	//user := UserRecord{
	//	Name:    "Lee",
	//	Phone:   "23121313",
	//	Address: "fsdfsdfsdfsdf",
	//}
	//println(d.insertUserRecord(insertSqlString, user))
	//d.selectUserRecordById(selectSqlString, 1)

	d.getRecord(1)

	defer d.Close()
}
