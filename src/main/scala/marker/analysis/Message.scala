package marker.analysis {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.rdd.RDD

  class Message(val from: String, val message: String) {
    def tokenize(ss: SparkSession, terms: RDD[(String, String)]) = {
      val wordOccurances = ss.sparkContext.
        parallelize(message.map(char => if (char.isLetter) char else ' ').split(' ')).
        filter(word => word.length > 0).
        map(word => (word, 1)).
        reduceByKey(_+_)
      val tokenized = terms.
        cogroup(wordOccurances).
        map{case (_, vals) => vals}.
        filter{case (word, value) => word.nonEmpty && value.nonEmpty}.
        map{case (word, value) => (word.head, value.head)}
      tokenized
    }
  }

}