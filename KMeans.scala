import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KMeans {
  type Point = (Double, Double)

  var centroids: Array[Point] = Array[Point]()
  def distance(centroid: Point, point: Point): Double = {
    val d = Math.sqrt(
      (point._1 - centroid._1) * (point._1 - centroid._1) + (point._2 - centroid._2) * (point._2 - centroid._2)
    )
    d
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("centroids")
    val sc = new SparkContext(conf)

    /* read initial centroids from centroids.txt */
    centroids = sc
      .textFile(args(1))
      .map(line => {
        val a = line.split(",")
        (a(0).toDouble, a(1).toDouble)
      })
      .collect()

    val points = sc
      .textFile(args(0))
      .map(line => {
        val a = line.split(",")
        (a(0).toDouble, a(1).toDouble)
      })

    points.foreach(println)
    centroids.foreach(println)

    for (i <- 1 to 5) {
      val cs = sc.broadcast(centroids)
      centroids = points
        .map { p =>
          (cs.value.minBy(distance(p, _)), p)
        }
        .groupByKey()
        .map {
          /* ... calculate a new centroid ... */
          case (existingCentroid, listOfPoints) =>
            var count = 0
            var sumx = 0.0
            var sumy = 0.0
            for (p <- listOfPoints) {
              sumx = sumx + p._1
              sumy = sumy + p._2
              count = count + 1

            }

            var cx = sumx / count
            var cy = sumy / count
            (cx, cy)
        }
        .collect
    }

    centroids.foreach(println)
  }
}