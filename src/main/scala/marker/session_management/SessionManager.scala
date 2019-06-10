package marker.session_management

import org.apache.spark.sql.SparkSession
import marker.analysis.Model
import marker.analysis.Message
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SessionManager {
  val spark = SparkSession.builder.appName("Marker").getOrCreate()
  val conf = new SparkConf().setMaster("local[2]").setAppName("Marker")
  val ssc = new StreamingContext(conf, Seconds(1))
  val lines = AkkaUtils.createStream[String](ssc, Props[CustomActor](), "CustomReceiver")
  val model = Model

  def main(args: Array[String]): Unit = {
    
  }

  def shutdown(): Unit = {
    model.saveAndQuit()
  }
}

class CustomActor extends ActorReceiver {
  import SessionManager.model

  def receive = {
    case _: InMessage(from, text) =>  model.passMessage(from, text)
  }
}