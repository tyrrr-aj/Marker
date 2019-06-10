package marker.analysis

import java.io.File

import marker.session_management.SessionManager.{model, spark}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Model {
  val clientsPath = "c:\\Spark\\Apps\\marker\\src\\main\\resources\\RDDs\\clients"
  val termsPath = "c:\\Spark\\Apps\\marker\\src\\main\\resources\\RDDs\\terms"
  val termValuesPath = "c:\\Spark\\Apps\\marker\\src\\main\\resources\\RDDs\\termValues"
  var terms = spark.sparkContext.sequenceFile(termsPath)
  var termValues = spark.sparkContext.sequenceFile(termValuesPath)
  var clients = spark.sparkContext.sequenceFile(clientsPath)
  val alfa = 0.3
  val beta = 0.5
  val refLevel = 5

  def passMessage(speaker: String, text: String) = {
    val message = new Message(speaker, text)
    val tokenized = message.tokenize(spark, terms)
    model.adjustSuspicionLevels(tokenized, speaker)
  }

  def adjustSuspicionLevels(tokenizedMessage: RDD[(String, Int)], speaker: String) = {
    val speakerSLevel = clients.filter{case (key, _) => key == speaker}.collect()(0)._2
    adjustClientSuspicionLevel(tokenizedMessage, speaker)
    adjustTermsSuspicionLevel(tokenizedMessage, speakerSLevel)
  }

  private def adjustClientSuspicionLevel(tokenizedMessage: RDD[(String, Int)],
                                 speaker: String)= {
    clients = clients.map{case (client, sLevel) => if (client == speaker) (client, sLevel + calculateClientModifier(tokenizedMessage)) else (client, sLevel)}
  }

  private def calculateClientModifier(tokenizedMessage: RDD[(String, Int)]) = {
    var modifier = spark.sparkContext.longAccumulator("modifier")
    tokenizedMessage.cogroup(termValues).
      map{case (key, (occCont, sLvlCont)) => (occCont.head, sLvlCont.head)}.
      foreach{case (occurances, sLevel) => modifier += ((sLevel - refLevel) * beta).round.toInt}
    modifier.value
  }

  private def adjustTermsSuspicionLevel(tokenizedMessage: RDD[(String, Int)], speakerSLevel: Int) = {
    terms = termValues.cogroup(tokenizedMessage).map{case (term, (oldSLevel, sLevelMod)) => if (sLevelMod.nonEmpty)
      (term, (oldSLevel.head + sLevelMod.head * speakerSLevel * alfa).round.toInt) else (term, oldSLevel.head)}
  }

  def saveAndQuit() = {
    FileUtils.deleteDirectory(new File(termsPath))
    terms.saveAsSequenceFile(termsPath)
    FileUtils.deleteDirectory(new File(termValuesPath))
    termValues.saveAsSequenceFile(termValuesPath)
    FileUtils.deleteDirectory(new File(clientsPath))
    clients.saveAsSequenceFile(clientsPath)
    spark.stop()
  }
}
