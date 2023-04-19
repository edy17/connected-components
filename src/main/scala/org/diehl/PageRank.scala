package org.diehl

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.spark.HashPartitioner

import scala.collection.immutable.ListMap

object PageRank {

  val HADOOP_NAME_NODE = "172.27.176.1:9000"
  val SPARK_MASTER = "172.27.176.1:7077"
  val log = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {

    log.setLevel(Level.WARN)

    val conf = new SparkConf(false)
    conf.set("spark.app.name", "page-rank")
    conf.set("spark.master", s"spark://$SPARK_MASTER")

    conf.set("spark.driver.memory", "1g")
    conf.set("spark.driver.cores", "1")

    conf.set("spark.cores.max", "6")
    conf.set("spark.executor.instances", "2")
    conf.set("spark.executor.cores", "3")
    conf.set("spark.executor.memory", "2g")

    conf.set("spark.default.parallelism", "6")
    conf.set("spark.sql.shuffle.partitions", "6")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    //Accumulateur permettant de compter les culs de sac
    val culDeSacCount: LongAccumulator = spark.sparkContext.longAccumulator("culDeSacCount")

    val timePerDataSet = testWithoutPartitioning(spark, culDeSacCount)
    val timeOptPerDataSet= testWithPartitioning(spark, culDeSacCount)

    val x = ListMap(timePerDataSet.toSeq.sortBy(_._1): _*)
    val y = ListMap(timeOptPerDataSet.toSeq.sortBy(_._1): _*)

    log.warn("Results without partitioning _____________________________________")
    x.foreach { entry =>
      log.warn("Dataset : " + entry._1 + ", Elapsed-time : " + entry._2)
    }

    log.warn("Results with partitioning _____________________________________")
    y.foreach { entry =>
      log.warn("Dataset : " + entry._1 + ", Elapsed-time : " + entry._2)
    }
    spark.stop()
  }

  def testWithoutPartitioning(spark: SparkSession, culDeSacCount: LongAccumulator): Map[Int, Long] = {
    var raw_data = spark.sparkContext.emptyRDD[(String, String)]
    var timePerDataSet: Map[Int, Long] = Map()
    for (i <- 1 to 12) {

      val data = spark.sparkContext.textFile(s"hdfs://$HADOOP_NAME_NODE/input/data/2012" + (if (i > 9) i else ("0" + i)) + ".csv")
        .map(x => {
          val row = x.split(",")
          (row(3), row(4))
        })
      raw_data = raw_data.union(data)
      val segments = raw_data.filter(x => !x._1.contains("ORIGIN_AIRPORT_ID"))
      val t0 = System.nanoTime()
      val (scores, culDeSac) = computePageRanks(segments.cache(), spark, culDeSacCount)
      val t1 = System.nanoTime()
      val time = (t1 - t0) / 1000000000
      timePerDataSet += (i -> time)
      log.warn(i + " Dataset(s) processed _____________________________________")
      log.warn("Elapsed time: " + time + " seconds")
      log.warn("Total nodes: " + scores.count())
      log.warn("Total edges: " + segments.count())
    }
    timePerDataSet
  }

  def testWithPartitioning(spark: SparkSession, culDeSacCount: LongAccumulator): Map[Int, Long] = {
    var raw_data = spark.sparkContext.emptyRDD[(String, String)]
    var timeOptPerDataSet: Map[Int, Long] = Map()
    for (i <- 1 to 12) {
      val data = spark.sparkContext.textFile(s"hdfs://$HADOOP_NAME_NODE/input/data/2012" + (if (i > 9) i else ("0" + i)) + ".csv")
        .map(x => {
          val row = x.split(",")
          (row(3), row(4))
        })
      raw_data = raw_data.union(data)
      val segments = raw_data.filter(x => !x._1.contains("ORIGIN_AIRPORT_ID"))
      val t0 = System.nanoTime()
      val (scores, culDeSac) = computePageRanks(segments.partitionBy(new HashPartitioner(6)).cache(), spark, culDeSacCount)
      val t1 = System.nanoTime()
      val time = (t1 - t0) / 1000000000
      timeOptPerDataSet += (i -> time)
      log.warn(i + " Dataset(s) processed _____________________________________")
      log.warn("Elapsed time: " + time + " seconds")
      log.warn("Total nodes: " + scores.count())
      log.warn("Total edges: " + segments.count())
    }
    timeOptPerDataSet
  }

  //Méthode permettant de supprimer les culs-de-sac trouvés de façon naive sur un graphe
  def removeCulDeSac(edges: RDD[(String, String)]): (RDD[(String, String)], RDD[String], Array[String]) = {
    val nodes = edges.flatMap(x => Array(x._1, x._2)).distinct()
    val links = edges.groupByKey() // RDD of (url, neighbors) pairs
    val culDeSacRDD = nodes.subtract(links.keys)
    var bestSegments = edges
    val culDeSacCollection = culDeSacRDD.collect()
    culDeSacCollection.foreach(x => {
      bestSegments = bestSegments.filter(y => !Array(y._1, y._2).contains(x))
    })
    (bestSegments, culDeSacRDD, culDeSacCollection)
  }

  //Méthode permettant de supprimer récursivement tous les culs-de-sac d'un graphe
  def removeAllCulDeSac(edges: RDD[(String, String)], culDeSacCount: LongAccumulator): (RDD[(String, String)], RDD[String], Array[String]) = {
    var (bestEdges, culDeSacRDD, culDeSacCollection): (RDD[(String, String)], RDD[String], Array[String]) = removeCulDeSac(edges)
    culDeSacCount.reset()
    var badNodesCount = culDeSacRDD.map(x => 1).fold(0)((acc, x) => acc + 1)
    while (badNodesCount != culDeSacCount.value) {
      culDeSacCount.reset()
      culDeSacCount.add(badNodesCount)
      val (newEdges, newCulDeSacRDD, newCulDeSacCollection): (RDD[(String, String)], RDD[String], Array[String]) = removeCulDeSac(bestEdges)
      bestEdges = newEdges
      culDeSacRDD = culDeSacRDD.union(newCulDeSacRDD)
      culDeSacCollection = Array.concat(culDeSacCollection, newCulDeSacCollection)
    }
    (bestEdges, culDeSacRDD, culDeSacCollection)
  }

  //Méthode de calcul de PageRank sur un graphe sans Cul-de-sac et avec gestion de piège dans la toile
  //On considère une constante de taxation de 0.85 que l’internaute continue à marcher et de 0.15  qu’il abandonne et recommence
  def computePageRanksOfConnectedGraph(edges: RDD[(String, String)]): RDD[(String, Double)] = {
    val maxError = 0.0001 //seuil de convergence
    val nodes = edges.flatMap(x => Array(x._1, x._2)).distinct()
    val nodesCount = nodes.count()
    val links = edges.groupByKey() // RDD of (url, neighbors) pairs
    var ranks = nodes.map(x => (x, 1D)) // RDD of (url, rank) pairs
    var oldRanks = nodes.map(x => (x, 0D))
    //Bloc de code à exécuter tant que le seuil de convergence n'est pas atteint
    //Le seuil de convergence correspond à la somme des erreurs entre les pageRank avant et après une itération
    while (oldRanks.join(ranks).map(x => (x._2._1 - x._2._2).abs).fold(0)((acc, x) => acc + x) > maxError) {
      oldRanks = ranks
      val contributions = links.join(ranks).flatMap {
        case (url, (neighbors, rank)) =>
          neighbors.map(dest => (dest, rank / neighbors.size))
      }
      ranks = contributions.reduceByKey(_ + _).mapValues(.15 +.85 * _)
    }
    //Normalisation to probabilities
    ranks.mapValues(_ / nodesCount)
  }

  //Méthode de calcul de PageRank sur un graphe avec gestion de piège dans la toile et de Cul-de-sac
  def computePageRanks(edges: RDD[(String, String)],
                       spark: SparkSession,
                       culDeSacCount: LongAccumulator): (RDD[(String, Double)], Array[String]) = {
    //On sépare les culs-de-sac du graphe en entré et on calcul les PageRank sur le graphe connecté resultant
    val (bestEdges, culDeSacRDD, culDeSacCollection): (RDD[(String, String)], RDD[String], Array[String]) = removeAllCulDeSac(edges, culDeSacCount)
    val links = edges.groupByKey() // RDD of (url, neighbors) pairs
    var ranks = computePageRanksOfConnectedGraph(bestEdges)
    //Si le graphe en entré contient des Culs-de-sac, on calcul leur pageRank à partir de ceux de leur connexion présent dans le graphe connecté
    //Le PageRank d'un cul-de-sac étant la somme des quotients de noeud entrant sur nombre d'enfants entrant dans le noeud
    if (culDeSacCollection.length > 0) {
      var culDeSacRanks = culDeSacRDD.map(x => (x, 0D))
      while (culDeSacRanks.filter(x => x._2 == 0).count() > 0) {
        val tmp = links.join(ranks.union(culDeSacRanks))
        var newCulDeSacRanks = spark.sparkContext.emptyRDD[(String, Double)]
        culDeSacCollection.foreach(x => {
          newCulDeSacRanks = tmp.filter {
            case (url, (neighbors, rank)) =>
              neighbors.count(y => x.equals(y)) >= 1
          }.map {
            case (url, (neighbors, rank)) =>
              (x, rank / neighbors.size)
          }.union(newCulDeSacRanks)
        })
        culDeSacRanks = newCulDeSacRanks.reduceByKey(_ + _)
      }
      ranks = ranks.union(culDeSacRanks)
    }
    (ranks.sortByKey(), culDeSacCollection)
  }
}
