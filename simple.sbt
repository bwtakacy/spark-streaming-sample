name := "Simple Project"


version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.4.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.1" exclude("org.spark-project.spark", "unused")

jarName in assembly := "my-project-assembly.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
