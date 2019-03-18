package TaskReport

import (
	"database/sql"
	"fmt"
	"github.com/ahworld07/DAG2yaml"
	"github.com/ahworld07/Taskconf"
	_ "github.com/mattn/go-sqlite3"
	"strconv"
	"strings"
	"errors"
)


func sameLen(inst string, lens int) string {
	minus := lens - len(inst)
	if minus > 0 {
		inst = inst + strings.Repeat(" ", minus)
	}
	return inst
}

func DBpath2conn(dbpath string)(sqlDb *sql.DB, err error){
	exit_file, err := DAG2yaml.PathExists(dbpath)
	if exit_file == false {
		err = errors.New(fmt.Sprintf("%s Not exists!", dbpath))
		return
	}
	sqlDb, err = sql.Open("sqlite3", dbpath)
	return
}

func ProjectStat(prjName, dbpath string)(string){
	exit_file, _ := DAG2yaml.PathExists(dbpath)
	if exit_file == false {
		return fmt.Sprintf("%s Not exists!", dbpath)
	}
	sqlDb, err := sql.Open("sqlite3", dbpath)
	DAG2yaml.CheckErr(err)

	tx, _ := sqlDb.Begin()
	defer tx.Rollback()
	var ProjectType, Status string
	var Total, Unsubmit, Pending, Running, Failed, Succeeded int
	//var Start_time, End_time time.Time
	rows, err := tx.Query("select ProjectType, Status, Total, Unsubmit, Pending, Running, Failed, Succeeded from project where ProjectName = ?", prjName)

	for rows.Next() {
		err = rows.Scan(&ProjectType, &Status, &Total, &Unsubmit, &Pending, &Running, &Failed, &Succeeded)
		DAG2yaml.CheckErr(err)
	}

	prjName = sameLen(prjName, 20)
	ProjectType = sameLen(ProjectType, 18)

	prjStat := fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" , prjName, ProjectType, sameLen(strconv.Itoa(Unsubmit),8), sameLen(strconv.Itoa(Pending),7), sameLen(strconv.Itoa(Running),7), sameLen(strconv.Itoa(Failed),6), sameLen(strconv.Itoa(Succeeded),9), sameLen(strconv.Itoa(Total),5), Status)
	return prjStat
}

func ProjectDepend(dbpath string){
	if dbpath == ""{
		return
	}
	exit_file, _ := DAG2yaml.PathExists(dbpath)
	if exit_file == false {
		panic(fmt.Sprintf("%s Not exists!", dbpath))
	}

	sqlDb, err := sql.Open("sqlite3", dbpath)
	DAG2yaml.CheckErr(err)

	tx, _ := sqlDb.Begin()
	defer tx.Rollback()
	var Modulen1, Modulen2 string
	//var Start_time, End_time time.Time
	rows, err := tx.Query("select Modulen1, Modulen2 from mbym")

	fmt.Println("Module depend:")
	for rows.Next() {
		err = rows.Scan(&Modulen1, &Modulen2)
		DAG2yaml.CheckErr(err)
		Modulen1 = sameLen(Modulen1, 25)
		mbym := fmt.Sprintf("%s\t%s" , Modulen1, Modulen2)
		fmt.Println(mbym)
	}
	//print last module
}

func ModuleReport(dbpath string){
	if dbpath == ""{
		return
	}
	exit_file, _ := DAG2yaml.PathExists(dbpath)
	if exit_file == false {
		panic(fmt.Sprintf("%s Not exists!", dbpath))
	}

	sqlDb, err := sql.Open("sqlite3", dbpath)
	DAG2yaml.CheckErr(err)

	tx, _ := sqlDb.Begin()
	defer tx.Rollback()
	var Mname, Status string
	var Total, Unsubmit, Pending, Failed, Succeeded, Running int
	//var Start_time, End_time time.Time
	rows, err := tx.Query("select Mname, Total, Unsubmit, Pending, Failed, Succeeded, Running, Status  from modulen")

	fmt.Println("Mname\tUnsubmit\tPending\tRunning\tFailed\tSucceeded\tTotal\tStatus")

	for rows.Next() {
		err = rows.Scan(&Mname, &Total, &Unsubmit, &Pending, &Failed, &Succeeded, &Running, &Status)
		DAG2yaml.CheckErr(err)
		Mname = sameLen(Mname, 24)
		mbym := fmt.Sprintf("%s\t%v\t%v\t%v\t%v\t%v\t%v\t%v" , Mname, Unsubmit, Pending, Running, Failed, Succeeded, Total, Status)
		fmt.Println(mbym)
	}
}


func ProjectReport(cff *Taskconf.ConfigFile){
	fmt.Println(fmt.Sprintf("%s\t%s\tUnsubmit\tPending\tRunning\tFailed\tSucceeded\tTotal\tStatus", sameLen("prjName", 20), sameLen("Type", 18)))
	for prjName, dbpath := range cff.Cfg.Section("project").KeysHash() {
		if prjName == ""{
			continue
		}
		prjStat := ProjectStat(prjName, dbpath)
		fmt.Println(prjStat)
	}
}

func GetAllPods(dbpath string){
	sqlDb, err := DBpath2conn(dbpath)
	tx, _ := sqlDb.Begin()
	defer tx.Rollback()
	rows, err := tx.Query("select Pod, Modulen, PodK8sId, Status, IniPath from job")

	var Pod, Modulen, PodK8sId, Status, IniPath string
	fmt.Println("Pod\tModulen\tIniPath\tStatus")

	for rows.Next() {
		err = rows.Scan(&Pod, &Modulen, &PodK8sId, &Status, &IniPath)
		DAG2yaml.CheckErr(err)
		fmt.Println(fmt.Sprintf("%s\t%s\t%s\t%s" , Pod, Modulen, PodK8sId, Status, IniPath))
	}
}

