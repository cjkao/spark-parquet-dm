package test.peter.scala.sample


import java.io.File
import com.peter.model._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.time.SpanSugar._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import scala.language.postfixOps

class NestedSuite extends FunSuite with Timeouts with BeforeAndAfter{

   @transient var sc: SparkContext = _
  before {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      sc = new SparkContext(conf)
    
    
  }

  after {
    if (sc != null) {
      sc.stop()
      sc = null
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
    }
  }
  test("filter map from readed parquet"){
      val outDir = "outers.parquet"
      val sqlContext = new SQLContext(sc)
      val dataFrame = sqlContext.read.parquet(outDir)
      val outers = dataFrame.map { row =>
         val key = row.getString(0)
         val inners = row.getAs[Seq[Row]](1).map(r => Inner(r.getString(0), r.getDouble(1)))
         val maps = row.getAs[Map[String,Int]](2)
         Outer(key, inners,maps)
      }
      val cnt=outers.filter(_.inners.length>1).count()
      assert(cnt==2)
      
  }
}