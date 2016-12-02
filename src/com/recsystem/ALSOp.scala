package com.recsystem

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by lee on 2016/11/14.
 */
object ALSOp {

  //var model:MatrixFactorizationModel = null
  var newsMap:Map[Int, (String)] = Map[Int,(String)]()

  //val userOptWeight = Map(1->0.2,2->0.2,3->0.3,4->0.3)
  //val logfolder="hdfs://sparkmaster:8020/user/rec/data"
  def train(sc:SparkContext, logfolder:String, userOptWeight:Map[Int,Double], alsmodelpath:String): Unit ={
    val userlog = sc.textFile(logfolder).map(_.replace("[","").replace("]","").split(","))
    val userscorehistory = userlog.map{p=>
      val userid = p(5)
      val rowkey = p(3)
      val score = userOptWeight.get(p(4).toInt).get
      ((userid, rowkey), score)
    }.reduceByKey((t,s)=>t+s)

    val newsFromlog = userlog.map{p=>
      val newsid = p(1)
      val newstype = p(2)
      val rowkey = p(3)
      (rowkey, newsid, newstype)
    }.collect

    for(news <- newsFromlog){
      newsMap +=  news._1.hashCode -> (news._2)
    }
    val trainingData  = userscorehistory.map{p=>
      val rowkeyhash = p._1._2.hashCode
      val useridhash = p._1._1.hashCode

      Rating(useridhash,rowkeyhash,p._2.toFloat)
    }

    var model = ALS.train(trainingData, 5, 5, 0.1)
    model.save(sc, alsmodelpath)
  }

  def getRecommand(sc:SparkContext, userid:String, top:Int, alsmodelpath:String ): Array[(String)] ={
    val model = MatrixFactorizationModel.load(sc, alsmodelpath)

    val reclist:Array[Rating] = model.recommendProducts(userid.hashCode,top)
    var rtn:ArrayBuffer[(String)] = ArrayBuffer[(String)]()
    for(rec <- reclist){
      val newsrowykeyhash = rec.product
      val newsrowkey = newsMap(newsrowykeyhash)
      rtn += newsrowkey
    }
    rtn.toArray
  }
}
