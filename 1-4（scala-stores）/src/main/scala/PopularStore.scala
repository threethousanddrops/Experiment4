import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object PopularStore {
   def main(args: Array[String]) {
     if (args.length < 3) {
       System.err.println("Usage: <input1 path> <input2 path> <output path>")
       System.exit(1)
     }

     val conf = new SparkConf().setAppName("Scala_PopularStore")
     val sc = new SparkContext(conf)
     val userLog = sc.textFile(args(0))
     val userInfo = sc.textFile(args(1))

     val infoProcessed = userInfo.filter(x=>(x.split(",").length==3))
     val nonfit = infoProcessed.filter(x=>(x.split(",")(1).equals("1")==false && x.split(",")(1).equals("2")==false && x.split(",")(1).equals("3")==false))
     val nonfitusers = nonfit.map(x=>(x.split(",")(0),1))//(id, 1)

     
     val rdd1 = userLog.filter(x=>x.split(",")(5).equals("1111"))
     val rdd2 = rdd1.filter(x=>x.split(",")(6).equals("0")==false)
     val users = rdd2.map(x=>(x.split(",")(0),x.split(",")(3)))//(id, merchant)

     val rdd = users.subtractByKey(nonfitusers)//

     val tmp = rdd.map(x=>(x._2,1)).reduceByKey(_+_)
     val result = tmp.map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1)).take(100)
     val out = sc.parallelize(result)
     out.saveAsTextFile(args(2))
     sc.stop()
    }
}


