// Databricks notebook source
case class EMP(empid:Int,empname:String,deptid:Int)
case class DEPT(deptid:Int,deptname:String)
object DEPT_TABLE{
  var dept:DEPT=new DEPT(0,"")
   def tableSql(): String = {
 "CREATE TABLE IF NOT EXISTS "+dept.getClass.getSimpleName+" ( deptid "+dept.deptid.getClass.getSimpleName+", deptname "+dept.deptname.getClass.getSimpleName+ " )"+" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'"
}
}
val empSchema: Seq[EMP] = Seq.empty[new EMP(10,"Name",10),new EMP(11,"Name1",11)]
val empdf = spark.createDataFrame(empSchema)
empdf.show()
val skipable_first_row = empdf.first() 
val useful_csv_rows    = empdf.filter(row => row != skipable_first_row)  
useful_csv_rows.show()
//Creates a temporary view using the DataFrame
useful_csv_rows.createOrReplaceTempView("tempEmp")
//empdf.registerTempTable("tempEmp")
var rows = spark.sql("SELECT empid, empname , deptid FROM tempEmp")
println(rows)
var count = spark.sql("SELECT count(*) count FROM tempEmp")
count.show()

 // create   table

spark.sql(DEPT_TABLE.tableSql())
 rows = spark.sql("SELECT deptid, deptname FROM dept")
println(rows)
 count = spark.sql("SELECT count(*) count FROM dept")
count.show()

spark.catalog.listTables().show()
