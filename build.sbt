name := "SparkTest"

version := "0.1"
scalaVersion := "2.11.12"
resolvers += Resolver.sonatypeRepo("public")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"

test in assembly := {}
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
//  println(cp)

  cp filter {_.data.toString.startsWith("/home/per/workspace/SparkTest/lib/")}
}


assemblyMergeStrategy in assembly := {
  case "manifest.mf"                                 => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
