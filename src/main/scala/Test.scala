/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
import java.io.File

import scala.io.Source._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


case class Person(id: Long, firstName: String, lastName: String) {}
case class Knows(from: Long, to: Long)

object TriangleQueryTest {

  private var socialnetwork_csv_folder: File = new File(".")

  private val NPARAMS = 1

  private def printUsage(): Unit = {
    val usage = """DFS Read-Write Test
                  |Usage: localFile dfsDir
                  |localFile - (string) local file to use in test
                  |""".stripMargin

    println(usage)
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != NPARAMS) {
      printUsage()
      System.exit(1)
    }

    var i = 0

    socialnetwork_csv_folder = new File(args(i))
    if (!socialnetwork_csv_folder.exists) {
      System.err.println(s"Given path (${args(i)}) does not exist")
      printUsage()
      System.exit(1)
    }

    if (!socialnetwork_csv_folder.isDirectory) {
      System.err.println(s"Given path (${args(i)}) is not a directory")
      printUsage()
      System.exit(1)
    }
  }

  def readCSVFile(spark: SparkSession, csvFile: File): DataFrame = {
    spark.read
    .format("csv")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .option("delimiter", "|")
    .load("file://" + csvFile.toString)
  }

  def main(args: Array[String]): Unit = {
    parseArgs(args)

    val spark = SparkSession
      .builder
      .appName("Triangle count test")
      .getOrCreate()
    import spark.implicits._

    val persons = readCSVFile(spark, new File(socialnetwork_csv_folder, "person_0_0.csv"))
      .as[Person].cache()
    val knows = readCSVFile(spark, new File(socialnetwork_csv_folder, "person_knows_person_0_0.csv"))
      .withColumnRenamed("Person.id0", "from")
      .withColumnRenamed("Person.id1", "to")
      .as[Knows].cache()
//    val example = List(Knows(1, 2), Knows(2, 3), Knows(3, 1))
//    val knows: Dataset[Knows] = example.toDF.as[Knows]
    println("Person count: " + persons.count)

//    knows.show(numRows = 10)

//    val r = knows.withColumnRenamed("from", "A2")
//        .withColumnRenamed("to", "B")
//    val s = knows.withColumnRenamed("from", "B")
//      .withColumnRenamed("to", "C")
//    val t = knows.withColumnRenamed("from", "C")
//      .withColumnRenamed("to", "A1")
//
//    val rs = r.join(s, "B")
//    val rst = rs.join(t, "C")
//    val triangles = rst.filter("A1 = A2")


    println("Knows count: " + knows.count)
    println(s"Knows storage level: ${knows.rdd.getStorageLevel.description}")

    val duos = knows.as("k1")
        .joinWith(knows.as("k2"), $"k1.to" === $"k2.from")
    val triangles = duos.joinWith(knows.as("k3"),
      condition = $"_2.to" === $"k3.from" && $"_1.from" === $"k3.to")
    triangles.explain(true)
    println(s"Triangle count: ${triangles.count()}")


    spark.stop()
  }
}
