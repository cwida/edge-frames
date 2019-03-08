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

import org.apache.spark.SparkConf

import scala.io.Source._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


case class Person(id: Long, firstName: String, lastName: String) {}
case class Knows(p1: Long, p2: Long)

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

  def findTriangles(spark: SparkSession, rel: Dataset[Knows]): Long = {
    import spark.implicits._
    val duos = rel.as("k1")
      .joinWith(rel.as("k2"), $"k1.p2" === $"k2.p1")
    val triangles = duos.joinWith(rel.as("k3"),
      condition = $"_2.p2" === $"k3.p2" && $"_1.p1" === $"k3.p1")
    triangles.explain(true)
    triangles.count()
  }

  def triangleCountTest(args: Array[String], spark: SparkSession): Unit = {
    parseArgs(args)
    import spark.implicits._
    val persons = readCSVFile(spark, new File(socialnetwork_csv_folder, "person_0_0.csv"))
      .as[Person].cache()
    val knows = readCSVFile(spark, new File(socialnetwork_csv_folder, "person_knows_person_0_0.csv"))
      .withColumnRenamed("Person.id0", "p1")
      .withColumnRenamed("Person.id1", "p2")
      .as[Knows].cache()
    val example = List(Knows(1, 2), Knows(2, 3), Knows(1, 3))
    val example_knows: Dataset[Knows] = example.toDF.as[Knows]
    println("Person count: " + persons.count)

    println("Knows count: " + knows.count)
    println(s"Knows storage level: ${knows.rdd.getStorageLevel.description}")

    val knows_is_ordered = knows.filter($"p2" < $"p1").isEmpty
    println(s"Knows is ordered, smaller person is the first attribute: $knows_is_ordered")


    println(s"Triangle count in real knows: ${findTriangles(spark, knows)}")
    println(s"Triangle count in example knows: ${findTriangles(spark, example_knows)}")

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[20]")
      .setAppName("Spark test")
      .set("spark.local.dir", "/scratch/per/spark-temp")
      .set("spark.executor.memory", "10g")


    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()


    triangleCountTest(args, spark)

    spark.stop()
  }
}
