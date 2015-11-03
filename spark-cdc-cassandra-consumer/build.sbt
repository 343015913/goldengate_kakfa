name := "SparkStreamingAggregation"

version := "0.2"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.0" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri") ,
  "org.apache.spark" %% "spark-core" % "1.4.0" % "provided" withSources() withJavadoc() , 
  "org.apache.spark" %% "spark-streaming" % "1.4.0" % "provided"  withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % "1.4.0" % "provided" withSources() withJavadoc() ,
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0" withSources() withJavadoc() ,
  "org.apache.spark" %% "spark-streaming-kafka" % "1.4.0" ,
  "com.github.nscala-time" %% "nscala-time" % "2.0.0" 
)


//mergeStrategy in assembly := {
//   case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
//      case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
 //       case "log4j.properties"                                  => MergeStrategy.discard
   //       case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
     //       case "reference.conf"                                    => MergeStrategy.concat
       //       case _                                                   => MergeStrategy.first
//}

