import org.apache.spark.sql.SparkSession

package marker.text_analysis {

class Message(val from: Int, val to: Int, val message: String) {
    def analyze(ss: SparkSession) {
      var data = ss.sparkContext.parallelize(message.split(" "))
    }
  }

}