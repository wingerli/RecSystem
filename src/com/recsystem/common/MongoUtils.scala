package com.recsystem.common

import com.mongodb.QueryBuilder
import com.mongodb.casbah.commons.{MongoDBList, MongoDBObject}
import com.mongodb.casbah.{MongoClient, WriteConcern => MongodbWriteConcern}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.partitioner.MongodbPartition
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.stratio.datasource.mongodb.MongodbContext

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON

/**
 * Created by lee on 2016/4/28.
 */
class MongoUtils extends Serializable{
  val logger = Logger.getLogger(this.getClass)
  val mongoIp = PropertiesUtils.getPropertiesValueByKey("mongoIp")
  val mongoPort = PropertiesUtils.getPropertiesValueByKey("mongoPort").toInt
  val mongoDb =PropertiesUtils.getPropertiesValueByKey("mongoDb")

  val builder = MongodbConfigBuilder(Map(Host -> List(mongoIp+":"+mongoPort.toString), Database -> mongoDb, Collection ->"user_interest_model", SamplingRatio -> 1.0, WriteConcern -> "normal"))
  val readConfig = builder.build()
  val builder_newNewsList = MongodbConfigBuilder(Map(Host -> List(mongoIp+":"+mongoPort.toString), Database -> mongoDb, Collection ->"new_news_list", SamplingRatio -> 1.0, WriteConcern -> "normal"))
  val readConfig_newNewsList = builder_newNewsList.build()

  val mongoClient = MongoClient(mongoIp, mongoPort)
  val collection_interest_model = mongoClient(mongoDb)("user_interest_model")
  val collection_rec_news = mongoClient(mongoDb)("user_rec_news")
  val collection_newNews_list = mongoClient(mongoDb)("new_news_list")

  case class KeyWords(keyword: String, weight: Double)
  case class UserInterest(time:String, topic: Array[Double], newsCnt: Long, keywords:Array[KeyWords])
  case class UserInterestModel(userid:String, interest: Array[UserInterest])
  case class UserRecNewsList(userid:String, recnews:Array[String])
  case class NewsInfo(doc_id:String, doc_type:String, rowkey:String, hasread:Boolean)
  case class NewNewsList(newslist: Array[NewsInfo])

  def main(args: Array[String]): Unit ={
    /*val sparkConf = new SparkConf()
    val sc = new SparkContext("local", "SparkExample", sparkConf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._

    val dataFrameDefault: DataFrame = createDataFrame(sc.parallelize(List(UserInterestModel("00000", Array(UserInterest("上午",Array(),0,Array(KeyWords("",0.0))),UserInterest("下午",Array(),0,Array(KeyWords("",0.))))))))
    val saveConfig = MongodbConfigBuilder(Map(Host -> List(mongoIp+":"+mongoPort.toString), Database -> mongoDb, Collection ->"user_interest_model", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))
    dataFrameDefault.saveToMongodb(saveConfig.build)

    val dataFrame: DataFrame = createDataFrame(sc.parallelize(List(UserInterestModel("1", Array(UserInterest("上午",Array(0.12,0.32),10,Array(KeyWords("体育",0.12))),UserInterest("下午",Array(0.44,0.45),20,Array(KeyWords("音乐",0.12))))))))

    val saveConfig = MongodbConfigBuilder(Map(Host -> List(mongoIp+":"+mongoPort.toString), Database -> mongoDb, Collection ->"user_interest_model", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))
    dataFrame.saveToMongodb(saveConfig.build)

    val userRecNewsSaveConfig = MongodbConfigBuilder(Map(Host -> List(mongoIp+":"+mongoPort.toString), Database -> mongoDb, Collection ->"user_rec_news", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))
    val userRecNewsListDf = createDataFrame(sc.parallelize(List(UserRecNewsList("00001",Array("")))))
    userRecNewsListDf.saveToMongodb(userRecNewsSaveConfig.build)

    val newNewsList = MongodbConfigBuilder(Map(Host -> List(mongoIp+":"+mongoPort.toString), Database -> mongoDb, Collection ->"new_news_list", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))
    val newNewsListDf = createDataFrame(sc.parallelize(List(NewNewsList(Array(
      NewsInfo("asdf2f2f11","1", "news0001", false),
      NewsInfo("asdf2f2f12","2", "news0002", false),
      NewsInfo("asdf2f2f13","1", "news0003", false),
      NewsInfo("asdf2f2f14","2", "news0004", false))))))
    newNewsListDf.saveToMongodb(newNewsList.build)

    val mongodbContext = new MongodbContext(sqlContext)
    val mongoRDD = mongodbContext.fromMongoDB(readConfig)
    mongoRDD.registerTempTable("user_interest_model")
    val uim = sqlContext.sql("SELECT interest FROM user_interest_model where userid=1").toJSON.collect
    val b = JSON.parseFull(uim(0)).get
    val c = b.asInstanceOf[Map[String, Any]]
    val interest = c.get("interest").get.asInstanceOf[List[Map[String, Any]]]*/
  }

  /**
   * 根据用户id和时间范围，获取用户的主题模型
   * @param sc
   * @param userid
   * @param time
   * @return
   */
  def getTopicByUserIdAndTime(sc:SparkContext, userid:String, time:String): Array[Double] ={
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    val mongodbContext = new MongodbContext(sqlContext)
    val mongoRDD = mongodbContext.fromMongoDB(readConfig)
    mongoRDD.registerTempTable("user_interest_model")
    logger.info("################userid="+userid+",time="+time)
    val uim = sqlContext.sql("SELECT interest FROM user_interest_model where userid="+userid).toJSON.collect
    var rtn = Array[Double]()
    if(uim != null && uim.length>0) {
      val b = JSON.parseFull(uim(0)).get
      val c = b.asInstanceOf[Map[String, Any]]
      if(c.get("interest") != None) {
        val interest = c.get("interest").get.asInstanceOf[List[Map[String, Any]]]
        for (map <- interest) {
          if(map.get("time") != None) {
            val timestr = map.get("time").get.toString
            if (time != null && time.equals(timestr)) {
              if(map.get("topic") != None) {
                val topic = map.get("topic").get.asInstanceOf[List[Double]]
                rtn = topic.asInstanceOf[List[Double]].toArray
              }
            }
          }
        }
      }
    }else{
      collection_interest_model.save(MongoDBObject(("userid",userid),("interest",MongoDBList(MongoDBObject(("time",time),("topic",Array()),("newsCnt",0L),("keywords",Array()))))))
    }
    rtn
  }

  /**
   * 获取用户历史阅读新闻篇数
   * @param sc
   * @param userid
   * @param time
   * @return
   */
  def getNewsCountByUserIdAndTime(sc:SparkContext, userid:String, time:String): Double={
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    val mongodbContext = new MongodbContext(sqlContext)
    val mongoRDD = mongodbContext.fromMongoDB(readConfig)
    mongoRDD.registerTempTable("user_interest_model")
    val uim = sqlContext.sql("SELECT interest FROM user_interest_model where userid='"+userid+"'").toJSON.collect
    var rtn = 0.0
    if(uim != null && uim.length>0) {
      val b = JSON.parseFull(uim(0)).get
      val c = b.asInstanceOf[Map[String, Any]]
      if(c.get("interest") != None) {
        val interest = c.get("interest").get.asInstanceOf[List[Map[String, Any]]]
        for (map <- interest) {
          if(map.get("time") != None) {
            val timestr = map.get("time").get.toString
            if (time != null && time.equals(timestr)) {
              if(map.get("newsCnt") != None) {
                val newsCnt = map.get("newsCnt").get
                rtn = newsCnt.toString.toDouble
                return rtn
              }
            }
          }
        }
      }
    }
    rtn
  }

  /**
   * 获取用户的关键词和权重
   * @param sc
   * @param userid
   * @param time
   * @return
   */
  def getUserKeywordsByUserIdAndTime(sc:SparkContext, userid:String, time:String): Map[String,Double] ={
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    val mongodbContext = new MongodbContext(sqlContext)
    val mongoRDD = mongodbContext.fromMongoDB(readConfig)
    mongoRDD.registerTempTable("user_interest_model")
    val uim = sqlContext.sql("SELECT interest FROM user_interest_model where userid='"+userid+"'").toJSON.collect
    var rtn: Map[String, Double] = Map()
    if(uim != null && uim.length>0) {
      val b = JSON.parseFull(uim(0)).get
      val c = b.asInstanceOf[Map[String, Any]]
      if(c.get("interest") != None) {
        val interest = c.get("interest").get.asInstanceOf[List[Map[String, Any]]]
        for (map <- interest) {
          if (map.get("time") != None) {
            val timestr = map.get("time").get.toString
            if (time != null && time.equals(timestr)) {
              if (map.get("keywords") != None) {
                val topic = map.get("keywords").get.asInstanceOf[List[Map[String, Any]]]
                for (map <- topic) {
                  val m = map.asInstanceOf[Map[String, Any]]
                  if(m.get("keyword") != None && m.get("weight") != None) {
                    rtn += (m.get("keyword").get.toString -> m.get("weight").get.toString.toDouble)
                  }
                }
              }
            }
          }
        }
      }
    }
    rtn
  }

  /**
   * 根据用户id和用户兴趣时间，更新主题兴趣模型
   * @param userid
   * @param time
   * @param topicArray
   */
  def updateTopicByUserIdAndTime(userid:String, time:String, topicArray:Array[Double], newsCount:Long): Unit ={
    val cnt = collection_interest_model.find(MongoDBObject(("userid",userid),("interest.time",time)))
    if(cnt.count()>0){
      collection_interest_model.update(QueryBuilder.start("userid").is(userid).and("interest.time").is(time).get(), MongoDBObject(("$set",MongoDBObject(("interest.$.topic",topicArray),("interest.$.newsCnt",newsCount)))))
    }else{
      collection_interest_model.update(QueryBuilder.start("userid").is(userid).get() ,MongoDBObject(("$push",MongoDBObject(("interest",MongoDBObject(("time",time),("topic",topicArray),("newsCnt",newsCount)))))))
    }

  }

  /**
   * 更新用户关键词
   * @param userid
   * @param time
   * @param keywordMap
   */
  def updateKeywordByuserIdAndTime(userid:String, time:String, keywordMap:Map[String, Double]): Unit ={
    val keywordsArray:Array[Map[String, Any]] = new Array(keywordMap.size)

    var i = 0
    for(key <- keywordMap.keySet){
      var map:Map[String, Any] = Map()
      map += ("keyword"->key)
      map += ("weight"->keywordMap.get(key))
      keywordsArray(i) = map
      i=i+1
    }
    val cnt = collection_interest_model.find(MongoDBObject(("userid",userid)))
    if(cnt.count()>0) {
      collection_interest_model.update(QueryBuilder.start("userid").is(userid).and("interest.time").is(time).get(), MongoDBObject(("$set", MongoDBObject(("interest.$.keywords", keywordsArray)))))
    }else{
      collection_interest_model.save(MongoDBObject(("interest",MongoDBList(MongoDBObject(("time",time),("keywords",keywordsArray)))),("userid",userid)))
    }
  }

  /**
   * 获取用户全部用户的主题向量和关键词向量
   * @param sc
   * @param time
   * @return
   */
  def getUserList(sc:SparkContext, time:String): Array[Map[String, Any]]={
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    val mongodbContext = new MongodbContext(sqlContext)
    val mongoRDD = mongodbContext.fromMongoDB(readConfig)
    mongoRDD.registerTempTable("user_interest_model")
    val uim = sqlContext.sql("SELECT * FROM user_interest_model ").toJSON.collect
    var rtn: List[Map[String, Any]] = List()
    for(a <- uim) {
      val b = JSON.parseFull(a).get
      val c = b.asInstanceOf[Map[String, Any]]
      if(c.get("userid") != None) {

        val userid = c.get("userid").get.toString
        var userMap: Map[String, Any] = Map()
        userMap += ("userid" -> userid)
        if(c.get("interest") != None) {
          val interest = c.get("interest").get.asInstanceOf[List[Map[String, Any]]]
          for (map <- interest) {
            val timestr = map.get("time").get.toString
            if (time != null && time.equals(timestr)) {
              if(map.get("topic") != None) {
                val topic = map.get("topic").get.asInstanceOf[List[Double]]
                val topicArray = topic.asInstanceOf[List[Double]].toArray
                userMap += ("topic" -> topicArray)
                if( map.get("keywords") != None) {
                  val keywords = map.get("keywords").get.asInstanceOf[List[Map[String, Any]]]
                  val keywordsArray: Array[String] = new Array(keywords.size)
                  var i = 0
                  for (map <- keywords) {
                    val m = map.asInstanceOf[Map[String, Any]]
                    if( m.get("keyword") != None) {
                      keywordsArray(i) = m.get("keyword").get.toString
                    }
                    i = i + 1
                  }
                  userMap += ("keyword" -> keywordsArray)
                }
              }
            }
          }
        }
        rtn = rtn.+:(userMap)
      }

    }
    rtn.toArray
  }

  /**
   * 更新用户的新闻推荐列表
   * @param userRecNewsList
   */
  def updateUserRecNewsList(userRecNewsList:Array[(String, Array[String])]): Unit ={
    for((userid, newslist) <- userRecNewsList){
     val cnt = collection_rec_news.find(MongoDBObject(("userid",userid)))
      val newsSaveList =MongoDBList()

      for(doc_id <- newslist){
        newsSaveList += MongoDBObject(("doc_id",doc_id))
      }

      if(cnt.count()>0){
        collection_rec_news.update(QueryBuilder.start("userid").is(userid).get(), MongoDBObject(("$push",MongoDBObject(("recnews",newsSaveList)))))
      }else{
        collection_rec_news.save(MongoDBObject(("recnews",newsSaveList),("userid",userid)))
      }
    }
  }

  /**
   * 获取新的新闻列表
   * @param sc
   * @return
   */
 /* def getNewNewsList(sc:SparkContext): Array[(Seq[String],Seq[String], Seq[String])] ={
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    val mongodbContext = new MongodbContext(sqlContext)
    val mongoRDD = mongodbContext.fromMongoDB(readConfig_newNewsList)
    mongoRDD.registerTempTable("new_news_list")
    val uim = sqlContext.sql("SELECT newslist FROM new_news_list").toJSON.collect
    val newsa = uim(0)
    val newsb = JSON.parseFull(newsa).get
    val newsc = newsb.asInstanceOf[Map[String, Any]]
    val rtn:Array[(Seq[String],Seq[String], Seq[String])] = new Array(1)
    if(newsc.get("newslist") != None) {
      val newsList = newsc.get("newslist").get.asInstanceOf[List[Map[String, Any]]]
      var i = 0
      val doc_typeSeq:Array[String] = new Array(newsList.length)
      val rowkeySeq:Array[String] = new Array(newsList.length)
      var doc_idSeq:Array[String]= new Array(newsList.length)
      for(news <- newsList){
        val newsmap = news.asInstanceOf[Map[String, Any]]
        if(newsmap.get("hasread") != None && newsmap.get("hasread").get.asInstanceOf[Boolean] == false){
          var doc_type = ""
          var rowkey = ""
          var doc_id = ""
          if(newsmap.get("rowkey") != None){
            rowkey = newsmap.get("rowkey").get.toString
          }
          if(newsmap.get("doc_type") != None){
            doc_type = newsmap.get("doc_type").get.toString
          }
          if(newsmap.get("doc_id") != None){
            doc_id = newsmap.get("doc_id").get.toString
          }
          doc_typeSeq(i) = doc_type
          rowkeySeq(i) = rowkey
          doc_idSeq(i) = doc_id
          i = i + 1
        }
      }

      val doc_typeSeqNew:Array[String] = new Array(i)
      val rowkeySeqNew:Array[String] = new Array(i)
      var doc_idSeqNew:Array[String]= new Array(i)
      for(j <- 0 to i - 1){
        doc_typeSeqNew(j) = doc_typeSeq(j)
        rowkeySeqNew(j) = rowkeySeq(j)
        doc_idSeqNew(j) = doc_idSeq(j)
      }
      rtn(0) = (doc_idSeqNew.toSeq, doc_typeSeqNew.toSeq, rowkeySeqNew.toSeq)
    }
    rtn
  }
*/
  /**
   * 更新处理完的新的新闻的hasread为true
   * @param sc
   * @param doc_idList
   * @param doc_typeList
   */
  /*def updateNewNewsListStatus(sc:SparkContext, doc_idList : Seq[String], doc_typeList:Seq[String]): Unit ={
    if(doc_idList != null && doc_idList.size>0){
      for(i <- 0 to doc_idList.length-1){
        collection_newNews_list.update(QueryBuilder.start("newslist.doc_id").is(doc_idList(i)).and("newslist.doc_type").is(doc_typeList(i)).get(), MongoDBObject(("$set",MongoDBObject(("newslist.$.hasread",true)))))
      }
    }

  }*/
}
