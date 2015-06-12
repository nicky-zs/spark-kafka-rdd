lazy val root = (project in file(".")).
  settings(
    organization := "me.zhongsheng",
    name := "spark-kafka-rdd",
    version := "1.5.0",
    scalaVersion := "2.10.4",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
    libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.1" % "compile",
    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  )
