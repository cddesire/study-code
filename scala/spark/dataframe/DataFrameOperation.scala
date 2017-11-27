
#### 使用SQLContext
``` scala
val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

```

#### 读取json
``` scala
val df = sqlContext.read.json("examples/src/main/resources/people.json")
// Displays the content of the DataFrame to stdout
df.show()
```


#### DataFrame Operations
``` scala
val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// Create the DataFrame
val df = sqlContext.read.json("examples/src/main/resources/people.json")

// Show the content of the DataFrame
df.show()
// age  name
// null Michael
// 30   Andy
// 19   Justin

// Print the schema in a tree format
df.printSchema()
// root
// |-- age: long (nullable = true)
// |-- name: string (nullable = true)

// Select only the "name" column
df.select("name").show()
// name
// Michael
// Andy
// Justin

// Select everybody, but increment the age by 1
df.select(df("name"), df("age") + 1).show()
// name    (age + 1)
// Michael null
// Andy    31
// Justin  20

// Select people older than 21
df.filter(df("age") > 21).show()
// age name
// 30  Andy

// Count people by age
df.groupBy("age").count().show()
// age  count
// null 1
// 19   1
// 30   1
```


#### Hive table
``` scala
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

// Queries are expressed in HiveQL
sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)
```


#### 重命名schema
``` scala
val colNames = Seq("uid", "col1", "col2", "col3", "col4", "col5", "col6")
val secondDF = firstDF.toDF(colNames: _*)

```


#### spark write hive table
``` scala
// 写入动态分区
df.write.partitionBy('year', 'month').saveAsTable(...)
readDF.write.mode(SaveMode.Overwrite).partitionBy("ds", "hh", "mi").insertInto(table)
// 写入静态分区
df.registerTempTable("demo");
hiveContext.sql("insert into test.test partition(date='20151205') select * from demo");
```
