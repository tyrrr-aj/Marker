package marker.session_management

import java.io.File

import org.apache.spark.sql.SparkSession
import marker.text_analysis.Message

object SessionManager {
  val spark = SparkSession.builder.appName("Marker").getOrCreate()
  var terms = spark.sparkContext.textFile("c:\\Spark\\Apps\\spy-chat\\src\\main\\resources\\RDDs\\terms")
  var termValues = spark.sparkContext.textFile("c:\\Spark\\Apps\\spy-chat\\src\\main\\resources\\RDDs\\termValues")
  var clients = spark.sparkContext.textFile("c:\\Spark\\Apps\\spy-chat\\src\\main\\resources\\RDDs\\clients")

  def main(args: Array[String]): Unit = {
    println("Hello")
    shutdown()
  }

  def passMessage(from: Int, to: Int, text: String): Unit = {
    val message = new Message(from, to, text)
    message.analyze(spark)
  }

  def shutdown(): Unit = {
    deleteRecursively(new File("c:\\Spark\\Apps\\spy-chat\\src\\main\\resources\\RDDs\\terms"))
    terms.saveAsTextFile("c:\\Spark\\Apps\\spy-chat\\src\\main\\resources\\RDDs\\terms")
    termValues.saveAsTextFile("c:\\Spark\\Apps\\spy-chat\\src\\main\\resources\\RDDs\\termValues")
    clients.saveAsTextFile("c:\\Spark\\Apps\\spy-chat\\src\\main\\resources\\RDDs\\clients")
    spark.stop()
  }

  def delDirectory(path: String): Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(path), true) }
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }
}
