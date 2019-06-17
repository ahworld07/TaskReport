package TaskReport

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/ahworld07/DAG2yaml"
	"github.com/ahworld07/dag"
	_ "github.com/mattn/go-sqlite3"
	"strconv"
	"strings"
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
		err = errors.New(fmt.Sprintf("%s Not exists!1", dbpath))
		return
	}
	sqlDb, err = sql.Open("sqlite3", dbpath)
	return
}

func ProjectStat(Id int, prjName, dbpath, ProjectBatch string)(string){
	exit_file, _ := DAG2yaml.PathExists(dbpath)
	if exit_file == false {
		return fmt.Sprintf("%s Not exists!2", dbpath)
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

	prjStat := fmt.Sprintf("%v%s\t%s\t%s\t%s%s\t%s\t%s\t%s%s\t%s" , sameLen(fmt.Sprintf("%v",Id), 7), prjName, ProjectType, ProjectBatch, sameLen(strconv.Itoa(Unsubmit),10), sameLen(strconv.Itoa(Pending),7), sameLen(strconv.Itoa(Running),7), sameLen(strconv.Itoa(Failed),6), sameLen(strconv.Itoa(Succeeded),11), sameLen(strconv.Itoa(Total),5), Status)
	return prjStat
}

func ProjectDepend(dbpath string){
	if dbpath == ""{
		return
	}
	exit_file, _ := DAG2yaml.PathExists(dbpath)
	if exit_file == false {
		panic(fmt.Sprintf("%s Not exists!3", dbpath))
	}

	sqlDb, err := sql.Open("sqlite3", dbpath)
	DAG2yaml.CheckErr(err)

	tx, _ := sqlDb.Begin()
	defer tx.Rollback()
	var Modulen1, Modulen2 string
	//var Start_time, End_time time.Time
	rows, err := tx.Query("select Modulen1, Modulen2 from mbym")
	TAsk_dag := dag.NewDAG()

	fmt.Println("Module depend:")
	for rows.Next() {
		err = rows.Scan(&Modulen1, &Modulen2)
		DAG2yaml.CheckErr(err)

		vertex1 := dag.NewVertex(Modulen1, Modulen1)
		ee1, _ := TAsk_dag.GetVertex(vertex1.ID)
		if ee1 == nil{
			err = TAsk_dag.AddVertex(vertex1)
			DAG2yaml.CheckErr(err)
		}else{
			vertex1 = ee1
		}

		vertex2 := dag.NewVertex(Modulen2, Modulen2)

		ee2, _ := TAsk_dag.GetVertex(vertex2.ID)
		if ee2 == nil{
			TAsk_dag.AddVertex(vertex2)
		}else{
			vertex2 = ee2
		}
		err = TAsk_dag.AddEdge(vertex1, vertex2)
		DAG2yaml.CheckErr(err)
	}

	for _, v := range TAsk_dag.Vertexs_slice(){
		module_str := v.ID
		vers,_ := TAsk_dag.Predecessors(v)

		for i, o := range vers{
			if i == 0{
				module_str = module_str + "[" + o.ID
			}else{
				module_str = module_str + ", " + o.ID
			}
		}
		if len(vers) > 0{
			module_str = module_str + "]"
		}

		fmt.Println(module_str)
	}
}

func ModuleReport(dbpath string){
	if dbpath == ""{
		return
	}

	exit_file, _ := DAG2yaml.PathExists(dbpath)
	if exit_file == false {
		panic(fmt.Sprintf("%s Not exists!4", dbpath))
	}else{
		fmt.Println(dbpath,"\n")
	}

	sqlDb, err := sql.Open("sqlite3", dbpath)
	DAG2yaml.CheckErr(err)

	tx, _ := sqlDb.Begin()
	defer tx.Rollback()
	var Mname, Status string
	var Total, Unsubmit, Pending, Failed, Succeeded, Running int
	//var Start_time, End_time time.Time
	rows, err := tx.Query("select Mname, Total, Unsubmit, Pending, Failed, Succeeded, Running, Status  from modulen")

	fmt.Println("Mname               \tUnsubmit\tPending\tRunning\tFailed\tSucceeded\tTotal\tStatus")

	for rows.Next() {
		err = rows.Scan(&Mname, &Total, &Unsubmit, &Pending, &Failed, &Succeeded, &Running, &Status)
		DAG2yaml.CheckErr(err)
		mbym := fmt.Sprintf("%s\t%v\t%v\t%v\t%v\t%v\t%v\t%v" , sameLen(Mname, 20), sameLen(strconv.Itoa(Unsubmit),8), sameLen(strconv.Itoa(Pending),7), sameLen(strconv.Itoa(Running),7), sameLen(strconv.Itoa(Failed),6), sameLen(strconv.Itoa(Succeeded),9), sameLen(strconv.Itoa(Total),5), Status)
		fmt.Println(mbym)
	}
	fmt.Println("\n")
}


func ProjectReport(projects_DBconn *sql.DB){
	fmt.Println(fmt.Sprintf("ID     %s\t%s\tBatch\tUnsubmit  Pending\tRunning\tFailed\tSucceeded  Total\tStatus", sameLen("prjName", 20), sameLen("Type", 18)))

	/*
	for prjName, dbpath := range cff.Cfg.Section("project").KeysHash(){
		if prjName == "000"{
			continue
		}
		//fmt.Println(fmt.Sprintf("%s1%s2",prjName, dbpath))
		prjStat := ProjectStat(prjName, dbpath)
		fmt.Println(prjStat)
	}
	*/
	var Id int
	var PrjName, DbPath, ProjectBatch string

	rows, err := projects_DBconn.Query("select Id, ProjectName, DbPath, ProjectBatch from projects")
	DAG2yaml.CheckErr(err)
	for rows.Next() {
		err = rows.Scan(&Id, &PrjName, &DbPath, &ProjectBatch)
		prjStat := ProjectStat(Id, PrjName, DbPath, ProjectBatch)
		fmt.Println(prjStat)
	}
}

func GetAllPods(dbpath string){
	sqlDb, err := DBpath2conn(dbpath)
	tx, _ := sqlDb.Begin()
	defer tx.Rollback()
	rows, err := tx.Query("select Pod, Modulen, PodK8sId, Status, Retriedtimes from job")

	var Pod, Modulen, PodK8sId, Status string
	var Retriedtimes int
	fmt.Println("Pod         \tModulen     \tPodK8sId                   \tStatus    \tRetriedtimes")

	for rows.Next() {
		err = rows.Scan(&Pod, &Modulen, &PodK8sId, &Status, &Retriedtimes)
		DAG2yaml.CheckErr(err)
		fmt.Println(fmt.Sprintf("%s\t%s\t%s\t%s\t%v" , sameLen(Pod, 12), sameLen(Modulen, 12), sameLen(PodK8sId, 27), sameLen(Status, 10), Retriedtimes))
	}
}

