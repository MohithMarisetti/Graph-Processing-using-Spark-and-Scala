import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math

  object Graph{
    def main(args: Array[String]){
      val conf = new SparkConf().setAppName("Assignment_5")
      val sc = new SparkContext(conf)
      

       var graph = sc.textFile("small-graph.txt").map( line => { val a = line.split(",")
                                                      (a(0).toLong,a(0).toLong,a.slice(1,a.length).toList.map(a => a.toLong)) })  
      
      for(i <- 1 to 5){
          graph = graph.flatMap( (i) => {            var buffer_list = new ListBuffer[(Long, Long)];                var temp_val = (i._1, i._2);      buffer_list += temp_val;      if (i._2 > 0) {        for (x <- i._3) {          temp_val = (x, i._2);          buffer_list += temp_val;        }      }      buffer_list;}).reduceByKey( (a,b) => {math.min(a,b)}).join(              graph.map((a) => {(a._1,a)})            ).map((v) => {(v._1,v._2,v._3)});      }
  graph.map(g => (g.2, 1)).reduceByKey(+_).collect().foreach(println)
 }
}