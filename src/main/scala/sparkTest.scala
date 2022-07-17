import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkTest {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("yuanshenSpark")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.sqlContext.implicits._
    val rdd1 = sparkSession.sparkContext.makeRDD(List((1,"li",10),(2,"song",11),(1,"liu",12),(3,"qiang",13),(4,"cheng",14),(4,"ke",15)))
    val df1 = rdd1.toDF("id","name","age")
    df1.show()
  }
}
