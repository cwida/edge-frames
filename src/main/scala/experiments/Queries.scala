package experiments

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import sparkIntegration.implicits._

object Queries {
  val FIXED_SEED_1 = 42
  val FIXED_SEED_2 = 220

  def pathQueryNodeSets(rel: DataFrame, selectivity: Double = 0.1): (DataFrame, DataFrame) = {
    (rel.selectExpr("src AS a").sample(selectivity, FIXED_SEED_1),
      rel.selectExpr("dst AS z").sample(selectivity, FIXED_SEED_2))
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
      r = r.where(new Column(c(0)) !== new Column(c(1)))
    }
    r
  }

  def twoPathBinaryJoins(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val relLeft = rel.selectExpr("src AS a", "dst AS b").join(nodeSet1, Seq("a"), "left_semi")
    val relRight = rel.selectExpr("dst AS z", "src AS b").join(nodeSet2, Seq("z"), "left_semi")

    withDistinctColumns(relLeft.join(relRight, Seq("b")).selectExpr("a", "b", "z"), Seq("a", "b", "z"))
  }

  def twoPathPattern(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val twoPath = rel.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (z)
      """.stripMargin, Seq("a", "z", "b"))
    // TODO should be done before the join
    val filtered = twoPath.join(nodeSet1, Seq("a"), "left_semi")
      .join(nodeSet2, Seq("z"), "left_semi")
      .select("a", "b", "z")
    withDistinctColumns(filtered, Seq("a", "b", "z"))
  }

  def threePathBinaryJoins(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val relLeft = rel.selectExpr("src AS a", "dst AS b").join(nodeSet1, Seq("a"), "left_semi")
    val relRight = rel.selectExpr("dst AS z", "src AS c").join(nodeSet2, Seq("z"), "left_semi")

    val middleLeft = relLeft.join(rel.selectExpr("src AS b", "dst AS c"), Seq("b")).selectExpr("a", "b", "c")
    relRight.join(middleLeft, "c").select("a", "b", "c", "z")
  }

  def threePathPattern(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val threePath = rel.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c);
        |(c) - [] -> (z)
      """.stripMargin, Seq("a", "z", "c", "b"))
    threePath.join(nodeSet1, Seq("a"), "left_semi")
      .join(nodeSet2, Seq("z"), "left_semi")
      .select("a", "b", "c", "z")
  }

  def fourPathBinaryJoins(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val relLeft = rel.selectExpr("src AS a", "dst AS b").join(nodeSet1, Seq("a"), "left_semi")
    val relRight = rel.selectExpr("dst AS z", "src AS d").join(nodeSet2, Seq("z"), "left_semi")

    val middleLeft = relLeft.join(rel.selectExpr("src AS b", "dst AS c"), Seq("b")).selectExpr("a", "b", "c")
    val middleRight = relRight.join(rel.selectExpr("src AS c", "dst AS d"), Seq("d")).selectExpr("c", "d", "z")
    withDistinctColumns(middleRight.join(middleLeft, "c").select("a", "b", "c", "d", "z"), Seq("a", "b", "c", "d", "z"))
  }

  def fourPathPattern(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame) = {
    val leftRel = rel.join(nodeSet1.selectExpr("a AS src"), Seq("src"), "left_semi")
    val rightRel = rel.join(nodeSet2.selectExpr("z AS dst"), Seq("dst"), "left_semi")
      .select("src", "dst")  // Necessary because Spark reorders the columns

    val fourPath = rel.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c);
        |(c) - [] -> (d);
        |(d) - [] -> (z)
      """.stripMargin, Seq("a", "z", "b", "d", "c"),
      Seq(leftRel,
        rel.alias("edges_2"),
        rel.alias("edges_3"),
        rightRel
      )
    )

    withDistinctColumns(fourPath.join(nodeSet1, Seq("a"), "left_semi")
      .join(nodeSet2, Seq("z"), "left_semi")
      .select("a", "b", "c", "d", "z"), Seq("a", "b", "c", "d", "z"))
  }

  def triangleBinaryJoins(spark: SparkSession, rel: DataFrame): DataFrame = {
    import spark.implicits._

    val duos = rel.as("R")
      .joinWith(rel.as("S"), $"R.dst" === $"S.src")
    val triangles = duos.joinWith(rel.as("T"),
      condition = $"_2.dst" === $"T.dst" && $"_1.src" === $"T.src")

    triangles.selectExpr("_2.src AS a", "_1._1.dst AS b", "_2.dst AS c")
  }

  def trianglePattern(rel: DataFrame): DataFrame = {
    rel.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c);
        |(a) - [] -> (c)
        |""".stripMargin, List("a", "b", "c"))
  }
}
