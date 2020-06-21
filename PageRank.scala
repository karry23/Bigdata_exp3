import org.apache.spark._
object PageRank {
  def rank(sc: SparkContext, inputPath: String) = {
    val inputFile = sc.textFile(inputPath)
    val links = inputFile.map(x => x.split("\t")).map(x => (x(0), x(1).split(",")))
    //初始值全部设置为1.0
    var ranks = links.mapValues(v => 1.0)

    for(i <- 0 until 10) {
      val cont = links.join(ranks).flatMap {
        //(pageId,(links,pr))
        //还是一样的逻辑，对于边v->u
        //产生一个键值对(u,pr(v)/v.linksize)
        case (pId, (l,r)) => l.map(dest => (dest, r / l.size))
      }
      //reduceByKey将同key的入边pr全部累加，并通过mapValues实现随机浏览模型
      ranks = cont.reduceByKey((x,y) => x+y).mapValues { v => 0.15 + 0.85 * v }
    }
    ranks.map(p => (p._1,p._2.formatted("%.10f"))).sortBy(_._2,false).saveAsTextFile("/home/hadoop/page_rank_result")
  }
  def main(args: Array[String]): Unit = {
    val inputPath = "hdfs://localhost:9000/exp3/input/DataSet.txt"
    val conf = new SparkConf().setAppName("hdfs://localhost:9000/exp2/output");
    val sc = new SparkContext(conf)
    rank(sc,inputPath)
  }
}