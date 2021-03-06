import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object AgeRatio {
   def main(args: Array[String]) {
     if (args.length < 2) {
       System.err.println("Usage: <input1 path> <input2 path> <output path>")
       System.exit(1)
     }

     val conf = new SparkConf().setAppName("Scala_AgeRatio")
     val sc = new SparkContext(conf)
     val inputLog = sc.textFile(args(0))
     val inputInfo = sc.textFile(args(1))

     val infoProcessed = inputInfo.filter(x=>(x.split(",").length==3))
     val infousers = infoProcessed.filter(x=>(x.split(",")(1).equals("1") || x.split(",")(1).equals("2") || x.split(",")(1).equals("3") || x.split(",")(1).equals("4") || x.split(",")(1).equals("5") || x.split(",")(1).equals("6") || x.split(",")(1).equals("7") || x.split(",")(1).equals("8")))
     val info = infousers.map(x=>(x.split(",")(0),x.split(",")(1)))//(id,age)
     
     val rdd = inputLog.filter(x=>x.split(",")(5).equals("1111"))
     val rdd2 = rdd.filter(x=>x.split(",")(6).equals("2")) 
     val users = rdd2.map(x=>(x.split(",")(0),1))
     val log = inputLog.map(x=>(x.split(",")(0),1))
     val notUsers = log.subtractByKey(users)

     val num = info.subtractByKey(notUsers)
     val result = num.map(x=>(x._2,1)).reduceByKey(_+_)
     result.saveAsTextFile(args(2))
     sc.stop()
    }
}