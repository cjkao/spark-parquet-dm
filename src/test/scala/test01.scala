package test.peter.scala.sample


import java.io.File

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.time.SpanSugar._
import org.apache.spark.SparkContext
//import org.apache.spark.util.Utils
import org.apache.spark.SparkConf
import scala.language.postfixOps
import scala.collection.mutable.Stack
class DriverSuite extends FunSuite with Timeouts with BeforeAndAfter{

   @transient var sc: SparkContext = _
  var stack: Stack[Int] = _
  before {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    stack = new Stack[Int]
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
  test("par 01"){
    
    val input = Seq("1,2,9,5", "2,7,5,6","4,5,3,4","6,7,5,6")
    val correctOutput = Array(
      (0, 1, 0.620299),
      (0, 2, -0.627215),
      (0, 3, 0.11776),
      (1, 2, -0.70069),
      (1, 3, 0.552532),
      (2, 3, 0.207514)
      )

    val inputRDD = sc.parallelize(input)
      //val customCorr = new CustomCorrelationJob().computeCorrelation(inputRDD, sc)
      val seqs = inputRDD.collect()
      assert (seqs.length ==4) 
//      inputRDD must_== correctOutput
  }
//  test("pop is invoked on a non-empty stack") {
//   
//      stack.push(1)
//      stack.push(2)
//      val oldSize = stack.size
//      val result = stack.pop()
//      assert(result === 2)
//      assert(stack.size === oldSize - 1)
//   }
// 
//  test("pop is invoked on an empty stack") {
// 
//    intercept[NoSuchElementException] {
//      stack.pop()
//    }
//    assert(stack.isEmpty)
//  }
  
//  
//  
//  test("driver should exit after finishing") {
//    val sparkHome = sys.env.get("SPARK_HOME").orElse(sys.props.get("spark.home")).get
//    // Regression test for SPARK-530: "Spark driver process doesn't exit after finishing"
//    val masters = Table(("master"), ("local"), ("local-cluster[2,1,512]"))
//    forAll(masters) { (master: String) =>
//      failAfter(60 seconds) {
//        Utils.executeAndGetOutput(
//          Seq("./bin/spark-class", "org.apache.spark.DriverWithoutCleanup", master),
//          new File(sparkHome),
//          Map("SPARK_TESTING" -> "1", "SPARK_HOME" -> sparkHome))
//      }
//    }
//  }
}

//abstract class SparkJobSpec extends SpecificationWithJUnit with BeforeAfterExample {
//
//  @transient var sc: SparkContext = _
//
//  def beforeAll = {
//    System.clearProperty("spark.driver.port")
//    System.clearProperty("spark.hostPort")
//
//    val conf = new SparkConf()
//      .setMaster("local")
//      .setAppName("test")
//    sc = new SparkContext(conf)
//  }
//
//  def afterAll = {
//    if (sc != null) {
//      sc.stop()
//      sc = null
//      System.clearProperty("spark.driver.port")
//      System.clearProperty("spark.hostPort")
//    }
//  }
//
//  override def map(fs: => Fragments) = Step(beforeAll) ^ super.map(fs) ^ Step(afterAll)
//
//}