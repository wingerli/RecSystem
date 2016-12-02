package com.recsystem.tmp

import java.text.SimpleDateFormat

import com.recsystem.common.HashingTFWithTerm
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizerModel, CountVectorizer, StopWordsRemover, RegexTokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.feature.{IDF, HashingTF}

import scala.collection.mutable

/**
 * Created by lee on 2016/4/5.
 */
object TestLDA {

  val conf = new SparkConf().setAppName("TestLDA")
  val sc = new SparkContext(conf)

  @transient
  val topicNumber = 10


  def main(args:Array[String]): Unit = {

  }

  def testLda(): Unit ={

    val input = Seq("hdfs://sparkmaster:8020/user/root/data/docs/1.txt","hdfs://sparkmaster:8020/user/root/data/docs/2.txt","hdfs://sparkmaster:8020/user/root/data/docs/3.txt","hdfs://sparkmaster:8020/user/root/data/docs/4.txt","hdfs://sparkmaster:8020/user/root/data/docs/5.txt")

    val (corpus, vocabArray, actualNumTokens) = preprocess(sc, input, 10000, "")

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(topicNumber).setOptimizer(new EMLDAOptimizer()).run(corpus)

    val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
    val topTopics = distLDAModel.topTopicsPerDocument(10)
    val sortedList = topTopics.map{case (newsId,topicList,weightList)=>
      val t = 10
      val array = new Array[Double](t)
      var i = 0
      for(p <-topicList){
        array(p) = weightList(i)
        i += 1
      }

      (newsId, array)
    }


    val descs = ldaModel.describeTopics


    // Output topics. Each is a distribution over words (matching word count vectors)
    /* println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
     val topics = ldaModel.topicsMatrix
     for (topic <- Range(0, 5)) {
       print("Topic " + topic + ":")
       for (word <- Range(0, ldaModel.vocabSize)) {
         print(" " + topics(word, topic));
       }
       println()
     }
 */

    //打印分析出的主题，以及关键词和权重
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
    println(s"${3} topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
      }
      println()
    }
  }

  def testWordCount(path: String): Array[(String, Int)] ={
    val input = "hdfs://sparkmaster:8020/user/root/data/docs/1.txt"
    val documents = sc.textFile(input).flatMap(_.split(" "))
    documents.map((_,1)).reduceByKey(_+_).sortBy(p=> p._2, false).take(10)
  }

  def testTfIdf(): Unit ={
    val input = Seq("hdfs://sparkmaster:8020/user/root/data/docs/1.txt","hdfs://sparkmaster:8020/user/root/data/docs/2.txt","hdfs://sparkmaster:8020/user/root/data/docs/3.txt","hdfs://sparkmaster:8020/user/root/data/docs/4.txt","hdfs://sparkmaster:8020/user/root/data/docs/5.txt")

    val documents: RDD[Seq[String]] = sc.textFile(input.mkString(",")).map(_.split(" ").toSeq)
    val hashingTF = new HashingTFWithTerm()
    val tf: RDD[(Vector,mutable.HashMap[Int, String])] = hashingTF.transforms(documents)
    val terms: mutable.HashMap[Int, String]= tf.map(p=>p._2).collect()(0)
    terms.get(125420)

    val ttf = tf.map(p=>p._1)

    ttf.cache()
    val idf = new IDF().fit(ttf)
    val tfidf: RDD[Vector] = idf.transform(ttf)

    val re = tfidf.collect()
    for (r <- re) {
      println(r)
    }

    val doc = tfidf.collect()
    val one = doc(0)
    val svv = one.asInstanceOf[SparseVector]
    val indices = svv.indices
    val values = svv.values

    val minMaxVals = tfidf.map{p=>
      val sv = p.asInstanceOf[SparseVector]
      (sv.values.min, sv.values.max)
    }
    val globalMinMax = minMaxVals.reduce{case ((min1, max1),(min2,max2)) =>
      (math.min(min1, min2), math.max(max1,max2))
    }
    println(globalMinMax)

  }


  def preprocess(
                  sc: SparkContext,
                  paths: Seq[String],
                  vocabSize: Int,
                  stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long) = {

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    // Get dataset of document texts
    // One document per line in each text file. If the input consists of many small files,
    // this can result in a large number of small partitions, which can degrade performance.
    // In this case, consider using coalesce() to create fewer, larger partitions.
    val df = sc.textFile(paths.mkString(",")).toDF("docs")
    val customizedStopWords: Array[String] = if (stopwordFile.isEmpty) {
      Array.empty[String]
    } else {
      val stopWordText = sc.textFile(stopwordFile).collect()
      stopWordText.flatMap(_.stripMargin.split("\\s+"))
    }
    val tokenizer = new RegexTokenizer()
      .setInputCol("docs")
      .setOutputCol("rawTokens")
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("rawTokens")
      .setOutputCol("tokens")
    stopWordsRemover.setStopWords(stopWordsRemover.getStopWords ++ customizedStopWords)
    val countVectorizer = new CountVectorizer()
      .setVocabSize(vocabSize)
      .setInputCol("tokens")
      .setOutputCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover, countVectorizer))

    val model = pipeline.fit(df)
    val documents = model.transform(df)
      .select("features")
      .map { case Row(features: Vector) => features }
      .zipWithIndex()
      .map(_.swap)

    (documents,
      model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary,  // vocabulary
      documents.map(_._2.numActives).sum().toLong) // total token count
  }


  def predict(documents: RDD[(Long, Vector)], ldaModel: LDAModel): Array[(Long, Vector)] = {
    var docTopicsWeight = new Array[(Long, Vector)](documents.collect().length)
    ldaModel match {
      case localModel: LocalLDAModel =>
        docTopicsWeight = localModel.topicDistributions(documents).collect()
      case distModel: DistributedLDAModel =>
        docTopicsWeight = distModel.toLocal.topicDistributions(documents).collect()
      case _ =>
    }
    docTopicsWeight
  }

  /*def wiki_LDA_example(sc: SparkContext): Unit ={
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val numTopics: Int = 100
    val maxIterations: Int = 100
    val vocabSize: Int = 10000

    /**
     * Data Preprocessing
     */
    // Load the raw articles, assign docIDs, and convert to DataFrame
    val rawTextRDD = ... // loaded from S3
    val docDF = rawTextRDD.zipWithIndex.toDF("text", "docId")

    // Split each document into words
    val tokens = new RegexTokenizer()
      .setGaps(false)
      .setPattern("\\p{L}+")
      .setInputCol("text")
      .setOutputCol("words")
      .transform(docDF)

    // Filter out stopwords
    val stopwords: Array[String] = ... // loaded from S3
    val filteredTokens = new StopWordsRemover()
        .setStopWords(stopwords)
        .setCaseSensitive(false)
        .setInputCol("words")
        .setOutputCol("filtered")
        .transform(tokens)

    // Limit to top `vocabSize` most common words and convert to word count vector features
    val cvModel = new CountVectorizer()
      .setInputCol("filtered")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .fit(filteredTokens)
    val countVectors = cvModel.transform(filteredTokens)
      .select("docId", "features")
      .map { case Row(docId: Long, countVector: Vector) => (docId, countVector) }
      .cache()

    /**
     * Configure and run LDA
     */
    val mbf = {
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      val corpusSize = countVectors.count()
      2.0 / maxIterations + 1.0 / corpusSize
    }
    val lda = new LDA()
      .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(math.min(1.0, mbf)))
      .setK(numTopics)
      .setMaxIterations(2)
      .setDocConcentration(-1) // use default symmetric document-topic prior
      .setTopicConcentration(-1) // use default symmetric topic-word prior

    val startTime = System.nanoTime()
    val ldaModel = lda.run(countVectors)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    /**
     * Print results.
     */
    // Print training time
    println(s"Finished training LDA model.  Summary:")
    println(s"Training time (sec)\t$elapsed")
    println(s"==========")

    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val vocabArray = cvModel.vocabulary
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.map(vocabArray(_)).zip(termWeights)
    }
    println(s"$numTopics topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) => println(s"$term\t$weight") }
      println(s"==========")
    }
  }
*/
}
