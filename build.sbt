name := "SparkTest"

version := "0.1"
scalaVersion := "2.11.12"
resolvers += Resolver.sonatypeRepo("public")

val gitCommitString = SettingKey[String]("gitCommit")

gitCommitString := git.gitHeadCommit.value.getOrElse("Not Set")

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](gitCommitString),
    buildInfoPackage := "experiments",
    buildInfoOptions += BuildInfoOption.ToMap)

// Compiler options
scalacOptions ++= Seq(
  //  "-encoding", "utf8", // Option and arguments on same line
  //  "-Xfatal-warnings",  // New lines for each options
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  //  "-language:higherKinds",
  //  "-language:existentials",
  "-language:postfixOps",
  "-optimize"
//  "-Xdisable-assertions"
)

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.17.1-s_2.11"

libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"


// Compile src's to Jar
test in assembly := {}
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {
    _.data.toString.startsWith("/home/per/workspace/SparkTest/lib/")
  }
}


assemblyMergeStrategy in assembly := {
  case "manifest.mf" => {
    MergeStrategy.discard
  }
  case x => {
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
  }
}

// Compile tests to jar.

Project.inConfig(Test)(baseAssemblySettings)
assemblyJarName in (Test, assembly) := s"${name.value}-test-${version.value}.jar"

test in (Test, assembly) := {}

mainClass in (Test, assembly) := Some("correctnessTesting.Runner")

assemblyExcludedJars in (Test, assembly) := {
  val cp = (fullClasspath in assembly).value
  cp filter {
    _.data.toString.startsWith("/home/per/workspace/SparkTest/lib/")
  }
}