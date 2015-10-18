import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.commons.io.FileUtils
import java.io._
import com.peter.model._
object ReadCode extends App {
    val outDir = "outers.parquet"
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  val dataFrame = sqlContext.read.parquet(outDir)
  val outers = dataFrame.map { row =>
     val key = row.getString(0)
     val inners = row.getAs[Seq[Row]](1).map(r => Inner(r.getString(0), r.getDouble(1)))
     val maps = row.getAs[Map[String,Int]](2)
     Outer(key, inners,maps)
  }
  print(outers.count())
  outers.collect().foreach(println)
  println("-----")
  outers.filter(_.inners.length>1).collect().foreach(println)
}