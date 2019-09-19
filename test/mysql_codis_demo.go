package main

import "database/sql"

func main() {
	db, dberr := sql.Open("mysql", "root:root@tcp(localhost:3306)/test?charset=utf8")
	defer db.Close()
	if dberr != nil {
		panic(dberr)
	}

}
