import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * @author sriparas
 */
object SimpleApp {
  def main(args: Array[String]){
    val logFile = "s3n://aws-sai-sriparasa/op/pig-kinesis/iteration_0/part-r-00000"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    
    val logData = sc.textFile(logFile, 3)
    logData.top(10)
    
    val output = "s3n://aws-sai-sriparasa/op/spark/op_iteration1"
    logData.saveAsTextFile(output)
  }
}