
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object HallOfFame {
    def main(args: Array[String]) {
       val conf = new SparkConf().
            setAppName("Missed Connections").
            setMaster("local")
        val sc = new SparkContext(conf)
       
        // Input
        val records = sc.textFile("~/Downloads/other/all").
            map { _.split(",") }.
            filter { _(0) != "playerID" }.
            keyBy { _(0) }
	
        // Output
        val text = data.map(x => { 
            val (k, v) = x;
            val (p, h) = v;
            p.mkString(",") + " :: " + h.mkString(",")
        })
        text.saveAsTextFile("out")

        // Shut down Spark, avoid errors.
        sc.stop()
    }
}

