package experiments

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import sparkIntegration.implicits._

sealed trait Query {
  def name: String

  override def toString: String = {
    name
  }

  def edges: Seq[(String, String)]

  def vertices: Set[String] = {
    edges.flatMap(e => Seq(e._1, e._2)).toSet
  }

  def applyPatternQuery(df: DataFrame, csrFilename: String): DataFrame

  def applyBinaryQuery(df: DataFrame, sp: SparkSession): DataFrame
}

/**
  * A query that cannot be executed. Used to decribe a query.
  * @param name
  * @param edges
  */
case class DescriptiveQuery(name: String, edges: Seq[(String, String)]) extends Query {
  override def applyPatternQuery(df: DataFrame, csrFilename: String = ""): DataFrame = ???

  override def applyBinaryQuery(df: DataFrame, sp: SparkSession): DataFrame = ???
}

case class Clique(size: Int) extends Query {
  override def name: String = {
    s"$size-clique"
  }

  override def edges: Seq[(String, String)] = {
    vertices.toSeq.sorted.combinations(2).filter(e => e(0) < e(1)).map(e => (e.head, e(1))).toSeq
  }

  override def vertices: Set[String] = {
    ('a' to 'z').take(size).map(c => s"$c").toSet
  }

  override def applyPatternQuery(df: DataFrame, csrFilename: String = ""): DataFrame = {
    Queries.cliquePattern(size, df, false, csrFilename)
  }

  override def applyBinaryQuery(df: DataFrame, sp: SparkSession): DataFrame = {
    Queries.cliqueBinaryJoins(size, sp, df)
  }
}

case class Cycle(size: Int) extends Query {

  override def name: String = {
    s"$size-cycle"
  }

  override def edges: Seq[(String, String)] = {
    val sortedVertices = vertices.toSeq.sorted
    sortedVertices.zipWithIndex.map {
      case (v1, i) => {
        (v1, sortedVertices((i + 1) % sortedVertices.size))
      }
    }
  }

  override def vertices: Set[String] = {
    ('a' to 'z').slice(0, size).map(c => s"$c").toSet
  }

  override def applyPatternQuery(df: DataFrame, csrFilename: String = ""): DataFrame = {
    Queries.cyclePattern(size, df)
  }

  override def applyBinaryQuery(df: DataFrame, sp: SparkSession): DataFrame = {
    Queries.cycleBinaryJoins(size, df)
  }
}

case class PathQuery(size: Int, selectivity: Double) extends Query {
  override def name: String = {
    s"$size-${"%.2f".format(selectivity)}-path"
  }

  override def edges: Seq[(String, String)] = {
    ???
  }

  override def applyPatternQuery(df: DataFrame, csrFilename: String = ""): DataFrame = {
    val (ns1, ns2) = Queries.pathQueryNodeSets(df, selectivity)
    Queries.pathPattern(size, df, ns1, ns2)
  }

  override def applyBinaryQuery(df: DataFrame, sp: SparkSession): DataFrame = {
    val (ns1, ns2) = Queries.pathQueryNodeSets(df, selectivity)
    Queries.pathBinaryJoins(size, df, ns1, ns2)
  }
}

/**
  * Diamond query as described in “Real-time Twitter Recommendation: Online Motif Detection in Large Dynamic Graphs”
  */
case class DiamondQuery() extends Query {

  override def name: String = {
    s"diamond"
  }

  override def edges: Seq[(String, String)] = {
    Seq(("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"))
  }

  override def applyPatternQuery(df: DataFrame, csrFilename: String = ""): DataFrame = {
    // TODO variable ordering
    df.findPattern(Queries.edgesToPatternString(edges), List("a", "b", "c", "d"), distinctFilter = true)
      .selectExpr("a", "b", "c", "d")
  }

  override def applyBinaryQuery(df: DataFrame, sp: SparkSession): DataFrame = {
    Queries.withDistinctColumns(df.selectExpr(s"src AS a", s"dst AS b")
      .join(df.selectExpr(s"src AS b", s"dst AS d"), s"b")
      .join(df.selectExpr("src AS a", "dst AS c"), "a")
      .join(df.selectExpr("src AS c", "dst AS d"), Seq("c", "d"), "left_semi"),
      this.vertices.toSeq.sorted)
      .selectExpr("a", "b", "c", "d")
  }
}

case class HouseQuery() extends Query {
  override def name: String = {
    "house"
  }

  override def edges: Seq[(String, String)] = Seq(
    ("a", "b"),
    ("b", "c"),
    ("c", "d"),
    ("a", "d"),
    ("a", "c"),
    ("b", "d"),
    ("c", "e"),
    ("d", "e")
  )

  override def applyPatternQuery(df: DataFrame, csrFilename: String = ""): DataFrame = {
    Queries.housePattern(df)
  }

  override def applyBinaryQuery(df: DataFrame, sp: SparkSession): DataFrame = {
    Queries.houseBinaryJoins(sp, df)
  }
}

case class KiteQuery() extends Query {

  override def name: String = {
    "kite"
  }

  override def edges: Seq[(String, String)] = Seq(
    ("a", "b"),
    ("a", "c"),
    ("b", "c"),
    ("b", "d"),
    ("c", "d")
  )

  override def applyPatternQuery(df: DataFrame, csrFilename: String = ""): DataFrame = {
    Queries.kitePattern(df)
  }

  override def applyBinaryQuery(df: DataFrame, sp: SparkSession): DataFrame = {
    Queries.kiteBinary(sp, df)
  }
}

object Queries {
  val FIXED_SEED_1 = 42
  val FIXED_SEED_2 = 220

  def pathQueryNodeSets(rel: DataFrame, selectivity: Double = 0.1): (DataFrame, DataFrame) = {
    (rel.selectExpr("src AS a").sample(selectivity, FIXED_SEED_1),
      rel.selectExpr("dst AS z").sample(selectivity, FIXED_SEED_2))
  }

  def edgesToPatternString(edges: Seq[(String, String)]): String = {
    edges.map(e => s"(${e._1}) - [] -> (${e._2})").mkString(";")
  }

  /**
    *
    * @param df
    * @param cols
    * @return `df` with filters such that all `cols` hold distinct values per row.
    */
  def withDistinctColumns(df: DataFrame, cols: Seq[String]): DataFrame = {
    var r = df
    for (c <- cols.combinations(2)) {
      r = r.where(new Column(c(0)) =!= new Column(c(1)))
    }
    r
  }

  def withSmallerThanColumns(df: DataFrame, cols: Seq[String]): DataFrame = {
    var r = df
    for (c <- cols.combinations(2)) {
      require(c(0) < c(1))
      r = r.where(new Column(c(0)) < new Column(c(1)))
    }
    r
  }

  def pathBinaryJoins(size: Int, ds: DataFrame, ns1: DataFrame, ns2: DataFrame): DataFrame = {
    size match {
      case 2 => {
        twoPathBinaryJoins(ds, ns1, ns2)
      }
      case 3 => {
        threePathBinaryJoins(ds, ns1, ns2)
      }
      case 4 => {
        fourPathBinaryJoins(ds, ns1, ns2)
      }
    }
  }

  def pathPattern(size: Int, ds: DataFrame, ns1: DataFrame, ns2: DataFrame): DataFrame = {
    val leftRel = ds.join(ns1.selectExpr("a AS src"), Seq("src"), "left_semi")
    val rightRel = ds.join(ns2.selectExpr("z AS dst"), Seq("dst"), "left_semi")
      .select("src", "dst") // Necessary because Spark reorders the columns

    val verticeNames = ('a' to 'z').toList.map(c => s"$c").slice(0, size) ++ Seq("z")

    val pattern = verticeNames.zipWithIndex.filter({ case (_, i) => {
      i < verticeNames.size - 1
    }
    }).map({ case (v1, i) => {
      s"($v1) - [] -> (${verticeNames(i + 1)})"
    }
    }).mkString(";")

    val variableOrdering = size match {
      case 2 => {
        Seq("a", "b", "z")
      }
      case 3 => {
        Seq("a", "b", "c", "z") // takes 0.285 against 3.975 for a, b, z, c on the first 500000 edges of the directed twitter set
      }
      case 4 => {
        Seq("a", "b", "c", "d", "z")
      }
    }

    val relationships = Seq(leftRel) ++ (0 until size - 2).map(i => ds.alias(s"edges$i")) ++ Seq(rightRel)
    ds.findPattern(pattern, variableOrdering, distinctFilter = true, smallerThanFilter = false, relationships, "")
      .selectExpr(verticeNames: _*)
  }

  private def twoPathBinaryJoins(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val relLeft = rel.selectExpr("src AS a", "dst AS b").join(nodeSet1, Seq("a"), "left_semi")
    val relRight = rel.selectExpr("dst AS z", "src AS b").join(nodeSet2, Seq("z"), "left_semi")

    withDistinctColumns(relLeft.join(relRight, Seq("b")).selectExpr("a", "b", "z"), Seq("a", "b", "z"))
  }

  private def threePathBinaryJoins(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val relLeft = rel.selectExpr("src AS a", "dst AS b").join(nodeSet1, Seq("a"), "left_semi")
    val relRight = rel.selectExpr("dst AS z", "src AS c").join(nodeSet2, Seq("z"), "left_semi")

    val middleLeft = relLeft.join(rel.selectExpr("src AS b", "dst AS c"), Seq("b")).selectExpr("a", "b", "c")
    withDistinctColumns(relRight.join(middleLeft, "c").select("a", "b", "c", "z"), Seq("a", "b", "c", "z"))
  }

  private def fourPathBinaryJoins(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val relLeft = rel.selectExpr("src AS a", "dst AS b").join(nodeSet1, Seq("a"), "left_semi")
    val relRight = rel.selectExpr("dst AS z", "src AS d").join(nodeSet2, Seq("z"), "left_semi")

    val middleLeft = relLeft.join(rel.selectExpr("src AS b", "dst AS c"), Seq("b")).selectExpr("a", "b", "c")
    val middleRight = relRight.join(rel.selectExpr("src AS c", "dst AS d"), Seq("d")).selectExpr("c", "d", "z")
    withDistinctColumns(middleRight.join(middleLeft, "c").select("a", "b", "c", "d", "z"), Seq("a", "b", "c", "d", "z"))
  }

  def cliqueBinaryJoins(size: Int, sp: SparkSession, ds: DataFrame): DataFrame = {
    size match {
      case 3 => {
        withSmallerThanColumns(triangleBinaryJoins(sp, ds), Seq("a", "b", "c"))
      }
      case 4 => {
        withSmallerThanColumns(fourCliqueBinaryJoins(sp, ds), Seq("a", "b", "c", "d"))
      }
      case 5 => {
        withSmallerThanColumns(fiveCliqueBinaryJoins(sp, ds), Seq("a", "b", "c", "d", "e"))
      }
      case 6 => {
        withSmallerThanColumns(sixCliqueBinaryJoins(sp, ds), Seq("a", "b", "c", "d", "e", "f"))
      }
    }
  }

  private def triangleBinaryJoins(spark: SparkSession, rel: DataFrame): DataFrame = {
    import spark.implicits._

    val duos = rel.as("R")
      .joinWith(rel.as("S"), $"R.dst" === $"S.src")
    val triangles = duos.joinWith(rel.as("T"),
      condition = $"_2.dst" === $"T.dst" && $"_1.src" === $"T.src")

    triangles.selectExpr("_2.src AS a", "_1._1.dst AS b", "_2.dst AS c")
  }


  // TODO run binaries on worst order
  private def fourCliqueBinaryJoins(sp: SparkSession, rel: DataFrame): DataFrame = {
    import sp.implicits._
    val triangles = triangleBinaryJoins(sp, rel)

    val fourClique =
      triangles.join(rel.alias("ad"), $"src" === $"a")
        .selectExpr("a", "b", "c", "dst AS d")
        .join(rel.alias("bd"), $"src" === $"b" && $"dst" === $"d", "left_semi")
        .join(rel.alias("cd"), $"src" === $"c" && $"dst" === $"d", "left_semi")
    fourClique.select("a", "b", "c", "d")
  }


  private def fiveCliqueBinaryJoins(sp: SparkSession, rel: DataFrame): DataFrame = {
    import sp.implicits._


    fourCliqueBinaryJoins(sp, rel)
      .join(rel.alias("ae"), $"src" === $"a")
      .selectExpr("a", "b", "c", "d", "dst AS e")
      .join(rel.alias("be"), $"src" === $"b" && $"dst" === $"e", "left_semi")
      .join(rel.alias("ce"), $"src" === $"c" && $"dst" === $"e", "left_semi")
      .join(rel.alias("de"), $"src" === $"d" && $"dst" === $"e", "left_semi")
      .select("a", "b", "c", "d", "e")
  }

  private def sixCliqueBinaryJoins(sp: SparkSession, rel: DataFrame): DataFrame = {
    import sp.implicits._


    fiveCliqueBinaryJoins(sp, rel)
      .join(rel.alias("af"), $"src" === $"a")
      .selectExpr("a", "b", "c", "d", "e", "dst AS f")
      .join(rel.alias("bf"), $"src" === $"b" && $"dst" === $"f", "left_semi")
      .join(rel.alias("cf"), $"src" === $"c" && $"dst" === $"f", "left_semi")
      .join(rel.alias("df"), $"src" === $"d" && $"dst" === $"f", "left_semi")
      .join(rel.alias("ef"), $"src" === $"e" && $"dst" === $"f", "left_semi")
      .select("a", "b", "c", "d", "e", "f")
  }

  val fiveVerticePermuations = ('a' to 'e').map(c => s"$c").permutations.toList
  var permCounter = 0

  def cliquePattern(size: Int, rel: DataFrame, useDistinctFilter: Boolean = false, csrFileName: String = ""): DataFrame = {
    //    val perm = fiveVerticePermuations(permCounter)
    //    permCounter += 1
    val alphabet = 'a' to 'z'
    val verticeNames = alphabet.slice(0, size).map(_.toString)

    val pattern = verticeNames.combinations(2).filter(e => e(0) < e(1))
      .map(e => s"(${e(0)}) - [] -> (${e(1)})")
      .mkString(";")
    //    println(s"Perm: ${perm.mkString(",")} at position $permCounter")
    rel.findPattern(pattern, verticeNames, distinctFilter = useDistinctFilter, smallerThanFilter = !useDistinctFilter, csrFileName)
  }

  def houseBinaryJoins(sp: SparkSession, rel: DataFrame): DataFrame = {
    import sp.implicits._

    withDistinctColumns(fourCliqueBinaryJoins(sp, rel)
      .join(rel.alias("ce"), $"src" === $"c")
      .selectExpr("a", "b", "c", "d", "dst AS e")
      .join(rel.alias("de"), $"src" === $"d" && $"dst" === $"e", "left_semi")
      .select("a", "b", "c", "d", "e"),
      Seq("a", "b", "c", "d", "e"))
  }

  def housePattern(rel: DataFrame): DataFrame = {
    rel.findPattern(edgesToPatternString(HouseQuery().edges), List("a", "b", "c", "d", "e"), distinctFilter = true)
  }

  def kiteBinary(sp: SparkSession, rel: DataFrame): DataFrame = {
    import sp.implicits._

    triangleBinaryJoins(sp, rel)
      .join(rel.alias("cd"), $"src" === $"c")
      .selectExpr("a", "b", "c", "dst AS d")
      .join(rel.alias("bd"), $"src" === $"b" && $"dst" === $"d", "left_semi")
      .filter($"c" < $"b" && $"b" < $"d" && $"d" < $"a")
      .selectExpr("a", "b", "c", "d")
  }

  def kitePattern(rel: DataFrame): DataFrame = {
    rel.findPattern(edgesToPatternString(KiteQuery().edges), List("c", "b", "d", "a"), smallerThanFilter = true)
      .selectExpr("a", "b", "c", "d")
  }

  def cycleBinaryJoins(size: Int, rel: DataFrame): DataFrame = {
    require(3 < size, "Use this method only for cyclics of four nodes and above")

    val verticeNames = ('a' to 'z').toList.map(c => s"$c")

    var ret = rel.selectExpr("src AS a", "dst AS b")
    for (i <- 1 until (size - 1)) {
      ret = ret.join(rel.selectExpr(s"src AS ${verticeNames(i)}", s"dst AS ${verticeNames(i + 1)}"), verticeNames(i))
    }
    ret = ret.join(rel.selectExpr(s"src AS ${verticeNames(size - 1)}", s"dst AS a"), Seq("a", verticeNames(size - 1)), "left_semi")
      .select(verticeNames.head, verticeNames.slice(1, size): _*)
    withDistinctColumns(ret, verticeNames.slice(0, size))
  }

  def cyclePattern(size: Int, rel: DataFrame): DataFrame = {
    require(3 < size, "Use this method only for cyclics of four nodes and above")

    val verticeNames = ('a' to 'z').toList.map(c => s"$c").slice(0, size)

    val pattern = verticeNames.zipWithIndex.map({ case (v1, i) => {
      s"($v1) - [] -> (${verticeNames((i + 1) % size)})"
    }
    }).mkString(";")


    val variableOrdering = size match {
      case 4 => {
        Seq("a", "b", "c", "d")
      }
      case 5 => {
        Seq("a", "b", "e", "c", "d")
      }
      case 6 => {
        Seq("a", "b", "f", "c", "e", "d")
      }
    }

    rel.findPattern(pattern, variableOrdering, distinctFilter = true)
  }

  implicit def queryRead: scopt.Read[Query] = {
    scopt.Read.reads(s => {
      val queryTypes = Seq("cycle", "clique", "path", "diamond", "house", "kite")

      queryTypes.find(t => s.startsWith(t)) match {
        case Some(t) => {
          val parameter = s.replace(t, "")
          t match {
            case "cycle" => {
              val size = parameter.toInt
              Cycle(size)
            }
            case "clique" => {
              val size = parameter.toInt
              Clique(size)
            }
            case "path" => {
              val parts = parameter.split('|')
              PathQuery(parts(0).toInt, parts(1).toDouble)
            }
            case "diamond" => {
              DiamondQuery()
            }
            case "house" => {
              HouseQuery()
            }
            case "kite" => {
              KiteQuery()
            }
            case _ => {
              println(s)
              throw new IllegalArgumentException(s"Unknown query: $s")
            }
          }
        }
        case None => {
          println(s)
          throw new IllegalArgumentException(s"Unknown query: $s")
        }
      }
    })
  }

}
