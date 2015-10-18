import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.commons.io.FileUtils
import java.io._
import com.peter.model._
//import org.apache.spark.sql.Row
 
 //see http://stackoverflow.com/questions/30008127/how-to-read-a-nested-collection-in-spark
object WriteParqNest extends App{
  
  
    val outDir = "outers.parquet"
    FileUtils.deleteDirectory(new File(outDir))
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    
    
    
    val data=List(
        Outer("k1", List(Inner("a", 3.1)), Map("a"-> 1, "b"->2)),
        Outer("Isat", List(Inner("c", 3.3),Inner("e", 3.2)), Map("a"-> 1, "b"->2)),
        Outer("Mar1", List(Inner("c", 3.3),Inner("d",2.2),Inner("e", 3.2)), Map("a"-> 1, "b"->2))
    
    )
    val outers = sc.parallelize(data)
    print(data)
    outers.toDF.write.parquet(outDir)
    

}
