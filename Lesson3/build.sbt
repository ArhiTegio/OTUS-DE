name := "json_reader_VVKuznetsov"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++=Seq("org.apache.spark" %% "spark-sql" % "2.4.0")
libraryDependencies ++=Seq("org.json4s" %% "json4s-jackson" % "3.2.11")