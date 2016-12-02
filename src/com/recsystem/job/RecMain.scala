package com.recsystem.job

import java.text.SimpleDateFormat
import java.util.Date

import com.recsystem.ALSOp
import com.recsystem.common.{HashingTFWithTerm, HbaseUtils, MongoUtils, PropertiesUtils}
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
 * Created by lee on 2016/4/18.
 */
object RecMain {
  val logger = Logger.getLogger(RecMain.getClass.getName)
  val hdfs = PropertiesUtils.getPropertiesValueByKey("hdfs_path")
  val recmodel = hdfs+PropertiesUtils.getPropertiesValueByKey("rec_model")
  val alsmodel = hdfs+PropertiesUtils.getPropertiesValueByKey("als_model")
  val dicPath = hdfs+PropertiesUtils.getPropertiesValueByKey("dic_path")

  val readWeightKey=PropertiesUtils.getPropertiesValueByKey("readWeightKey").toInt
  val commentWeightKey=PropertiesUtils.getPropertiesValueByKey("commentWeightKey").toInt
  val retweetWeightKey=PropertiesUtils.getPropertiesValueByKey("retweetWeightKey").toInt
  val storeWeightKey=PropertiesUtils.getPropertiesValueByKey("storeWeightKey").toInt
  val readWeight=PropertiesUtils.getPropertiesValueByKey("readWeight").toDouble
  val commentWeight=PropertiesUtils.getPropertiesValueByKey("commentWeight").toDouble
  val retweetWeight=PropertiesUtils.getPropertiesValueByKey("retweetWeight").toDouble
  val storeWeight=PropertiesUtils.getPropertiesValueByKey("storeWeight").toDouble
  val userOptWeight = Map(readWeightKey->readWeight,commentWeightKey->commentWeight,retweetWeightKey->retweetWeight,storeWeightKey->storeWeight)

  val topicsNumber = PropertiesUtils.getPropertiesValueByKey("topicsNumber").toInt
  val wordNumberTimes = PropertiesUtils.getPropertiesValueByKey("wordNumberTimes").toInt
  val similarityValue = PropertiesUtils.getPropertiesValueByKey("similarityValue").toDouble

  val lingchen = "凌晨"
  val shangwu = "上午"
  val xiawu = "下午"
  val wanshang = "晚上"

  val topicWeight = PropertiesUtils.getPropertiesValueByKey("topicWeight").toDouble
  val keywordWeight = PropertiesUtils.getPropertiesValueByKey("keywordWeight").toDouble

  val mongo = new MongoUtils()
  val hbase = new HbaseUtils()

  val hbasedatasizeforIDF =PropertiesUtils.getPropertiesValueByKey("hbasesizeforIdf").toLong

  val als_weight = PropertiesUtils.getPropertiesValueByKey("ASL_Weight").toDouble

  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("RecMain")
    val sc = new SparkContext(conf)

    if(args != null && args.length>0){
      for(i<- 0 to args.length-1){
        logger.info("#####################args["+i+"]="+args(i))
      }

      val comment = args(0)
      comment match{
        case "create_lda_model" =>{
          /******************************创建LDA模型*************************/
          //利用大量的新闻创建LDA模型，以及词库
          /*****************************************************************/
          val hbasedatasize = if(args(1) != null && "".equals(args(1))) args(1).toLong else -1L

          val allNews:Array[String] = hbase.loadAllNews(hbasedatasize)
          val brodcastAllNews = sc.broadcast(allNews).value
          createModel(sc, brodcastAllNews, topicsNumber, topicsNumber * wordNumberTimes)
        }
        case "update_user_model" =>{
          /*****************对于用户的兴趣模型训练流程************************/
          //根据时间分类：上午，下午，晚上，凌晨，每天4次进行用户的兴趣模型重新计算
          //还需要加上位置
          /*****************************************************************/
          //args(0)=update_user_model
          //args(1)=/user/root/data/userlogs/userlog.log
          val userlogpath = args(1)
          findOrUpdateUserModel(sc, userlogpath, hbasedatasizeforIDF)

          //更新ALS协同过滤模型
          ALSOp.train(sc, userlogpath, userOptWeight,alsmodel)
        }
        case "news_recommend" => {
          val userlogpath = args(1)
          newsRecommend(sc, userlogpath)
        }
        case "als_recommend" =>{
          als_recommend(sc)
        }

      }
    }
  }

  def newsRecommend(sc:SparkContext, userlogpath:String): Unit ={
    /***************对于新的新闻的处理流程*********************/
    //首先从mongodb中获取新的新闻列表，列表包括实际的新闻保存地址（这个地址是app可以访问到的），
    // 和切词之后的文件地址（保存在hdfs中的供推荐系统使用的）
    //可以定时做推荐列表处理，也可以用消息队列方式实时计算新的新闻
    /*******************************************************/
    //args(1)=/user/root/data/userlogs/userlog.log

    val zkServer = "sparkmaster:2181"
    val group = "rec"
    val topic = "rec"
    val num_thread = 3

    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("/tmp/kafka/rec")
    val kafkaStream = KafkaUtils.createStream(ssc, zkServer, group, Map(topic -> num_thread))
    val newsListArray = kafkaStream.map(_._2).map{p=>
      val newscontent= hbase.readNews(p)
      val userRecNewsList:Array[(String, Array[String])] = genRecommendList(sc, newscontent, p, hbasedatasizeforIDF)
      //更新用户的新闻推荐列表
      mongo.updateUserRecNewsList(userRecNewsList.toArray)
      logger.info("########################finished genRecommendList")
    }

    ssc.start()
    ssc.awaitTermination()

   /* val newsListArray:Array[(Seq[String],Seq[String],Seq[String])] = mongo.getNewNewsList(sc)
    val newsContentSeq:ArrayBuffer[String] = ArrayBuffer()
    var doc_typeSeq:Seq[String] = null
    var doc_idSeq:Seq[String] = null
    if(newsListArray != null && newsListArray.length>0){
      doc_typeSeq = newsListArray(0)._2
      val rowkeySeq = newsListArray(0)._3
      doc_idSeq = newsListArray(0)._1
      if(rowkeySeq != null && rowkeySeq.length>0){
        for(str <- rowkeySeq){
          newsContentSeq += hbase.readNews(str)
        }
      }

    }
    if(newsContentSeq != null && newsContentSeq != null) {
      logger.info("########################enter genRecommendList")
      val userRecNewsList:Array[(String, Array[(String,String)])] = genRecommendList(sc, newsContentSeq.toArray, doc_idSeq, doc_typeSeq, hbasedatasizeforIDF)

      val userFinalRecNewsList:ArrayBuffer[(String, Array[(String,String)])] = ArrayBuffer()
      //调用协同过滤算法

      for(userRecNews <- userRecNewsList){
        val newsArrayBuffer:ArrayBuffer[(String, String)] = ArrayBuffer()
        val userid = userRecNews._1
        val newslist = userRecNews._2
        var alsnewscnt = 1;
        //如果LDA推荐的新闻个数大于5篇，则ALS的篇数要乘以系数决定，否则默认推荐一篇ALS的
        if(newslist != null && newslist.size>5){
          alsnewscnt = (newslist.size * als_weight).toInt
        }
        //获取ALS推荐的新闻
        val alsNewsList = ALSOp.getRecommand(sc, userid, alsnewscnt,alsmodel)
        newsArrayBuffer.++=(newslist)
        newsArrayBuffer.++=(alsNewsList)
        userFinalRecNewsList += userid -> newsArrayBuffer.toArray
      }
      */


  }


  def als_recommend(sc:SparkContext): Unit ={
    val user:Array[Map[String, Any]] = mongo.getUserList(sc, "上午")

    var usercnt = 0
    val userList = sc.parallelize(user)
    userList.map {userMap=>
      val userid: String = userMap.get("userid").get.asInstanceOf[String]
      val alsNewsList = ALSOp.getRecommand(sc, userid, 2, alsmodel)
      val userRecNewsList:Array[(String, Array[String])] = Array{(userid,alsNewsList)}
      mongo.updateUserRecNewsList(userRecNewsList)
    }
  }

  /**
   * 利用所有文章进行LDA模型的训练，并保存模型和词袋
   * @param sc
   * @param folder
   * @param topicNumber
   */
  def createModel(sc: SparkContext, folder:Array[String], topicNumber:Integer, wordNumber: Integer): Unit ={
    val (corpus, vocabArray, actualNumTokens)  = preprocess(sc, folder, wordNumber, "")

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(topicNumber).setOptimizer(new EMLDAOptimizer()).run(corpus)
    ldaModel.save(sc, recmodel)

    val dic = sc.parallelize(vocabArray)
    dic.saveAsTextFile(dicPath)
  }

  /**
   * 根据用户浏览历史的日志行为，计算或更新用户的兴趣模型
   * @param sc
   * @param logfolder
   */
  def findOrUpdateUserModel(sc:SparkContext, logfolder:String, hbasedatasize:Long): Unit ={
    //log的格式: "date","docId","docType","rowkey","type","userid"
    //opttype: 1:阅读  2：评论  3：转发  4:收藏
    val userlog = sc.textFile(logfolder).map(_.replace("[","").replace("]","").split(","))
    val userlist = userlog.map(p=>p(5)).distinct().collect()
    logger.info("######################userlist.size="+userlist.size)
    for(user <- userlist) {
      logger.info("###########begin update model,user:"+user)
      //获取历史行为的用户操作列表
      val opttypeList = userlog.filter(p => p(5) == user).map(p => p(4)).collect()
      val opttypeArray = new Array[String](opttypeList.length)
      var i = 0
      for (opt <- opttypeList) {
        opttypeArray(i) = opt
        i = i + 1
      }
      logger.info("###########opttypeArray size:"+opttypeArray.size)

      //获取新闻内容
      val newsList = userlog.filter(p => p(5) == user).map(p => p(3)).collect()
      val newsArray = new Array[String](newsList.length)
      var j = 0
      for (news <- newsList) {
        val newscontent = hbase.readNews(news)
        logger.info("###########newscontent:"+newscontent)
        if(newscontent != null && !"".equals(newscontent)){
          newsArray(j) = newscontent
        }else{
          newsArray(j) = ""
        }
        j = j + 1
      }
      logger.info("###########newsArray size:"+newsArray.size)

      //获取用户行为的时间列表
      val dateList = userlog.filter(p => p(5) == user).map(p => p(0)).collect()
      val dateArray = new Array[String](dateList.length)
      var k = 0
      for (datetime <- dateList) {
        dateArray(k) = datetime
        k = k + 1
      }
      logger.info("###########dateArray size:"+dateArray.size)


      //首先计算每篇新闻历史记录的主题向量
      val newsVector: Array[(Vector)] = predictTopicForDoc(sc, newsArray)
      logger.info("###########newsVector size:"+newsVector.size)

      //获取用户历史兴趣主题模型
      var oldTopicWeight_lingchen: Array[Double] = mongo.getTopicByUserIdAndTime(sc, user, lingchen)
      var oldTopicWeight_shangwu: Array[Double] = mongo.getTopicByUserIdAndTime(sc, user, shangwu)
      var oldTopicWeight_xiawu: Array[Double] = mongo.getTopicByUserIdAndTime(sc, user, xiawu)
      var oldTopicWeight_wanshang: Array[Double] = mongo.getTopicByUserIdAndTime(sc, user, wanshang)
      logger.info("###########oldTopicWeight_lingchen size:"+oldTopicWeight_lingchen.size)
      logger.info("###########oldTopicWeight_shangwu size:"+oldTopicWeight_shangwu.size)
      logger.info("###########oldTopicWeight_xiawu size:"+oldTopicWeight_xiawu.size)
      logger.info("###########oldTopicWeight_wanshang size:"+oldTopicWeight_wanshang.size)
      //获取用户历史阅读新闻篇数
      val newsCounts_lingchen: Double = mongo.getNewsCountByUserIdAndTime(sc, user, lingchen)
      val newsCounts_shangwu: Double = mongo.getNewsCountByUserIdAndTime(sc, user, shangwu)
      val newsCounts_xiawu: Double = mongo.getNewsCountByUserIdAndTime(sc, user, xiawu)
      val newsCounts_wanshang: Double = mongo.getNewsCountByUserIdAndTime(sc, user, wanshang)
      logger.info("###########newsCounts_lingchen size:"+newsCounts_lingchen)
      logger.info("###########newsCounts_shangwu size:"+newsCounts_shangwu)
      logger.info("###########newsCounts_xiawu size:"+newsCounts_xiawu)
      logger.info("###########newsCounts_wanshang size:"+newsCounts_wanshang)

      //还原兴趣主题模型
      if (oldTopicWeight_lingchen != null && oldTopicWeight_lingchen.length > 0) {
        oldTopicWeight_lingchen=backDate(oldTopicWeight_lingchen, newsCounts_lingchen)
      }
      if (oldTopicWeight_shangwu != null && oldTopicWeight_shangwu.length > 0) {
        oldTopicWeight_shangwu=backDate(oldTopicWeight_shangwu, newsCounts_shangwu)
      }
      if (oldTopicWeight_xiawu != null && oldTopicWeight_xiawu.length > 0) {
        oldTopicWeight_xiawu=backDate(oldTopicWeight_xiawu, newsCounts_xiawu)
      }
      if (oldTopicWeight_wanshang != null && oldTopicWeight_wanshang.length > 0) {
        oldTopicWeight_wanshang=backDate(oldTopicWeight_wanshang, newsCounts_wanshang)
      }

      //按照用户的操作类型，根据操作权重，计算用户兴趣主题模型
      //opttype: 1:阅读  2：评论  3：转发 4:收藏 的权重分别为 1:0.2  2:0.2  3:0.3  4:0.3
      var pos = 0
      var cnt_lingchen = 0
      var cnt_shangwu = 0
      var cnt_xiawu = 0
      var cnt_wanshang = 0

      var userTopicWeight_lingchen = new Array[Double](newsVector(0).size)
      var userTopicWeight_shangwu = new Array[Double](newsVector(0).size)
      var userTopicWeight_xiawu = new Array[Double](newsVector(0).size)
      var userTopicWeight_wanshang = new Array[Double](newsVector(0).size)
      for(v:Vector <- newsVector){
        val array = v.toArray
        val time = checkDatetime(dateArray(pos))
        time match{
          case `lingchen` =>
            logger.info("###########match lingchen, time="+time)
            var k = 0
            for(a <- array){
              userTopicWeight_lingchen.update(k, userTopicWeight_lingchen(k) + a * userOptWeight.get(opttypeArray(pos).toInt).get)
              k = k+1
            }
            cnt_lingchen = cnt_lingchen + 1
          case `shangwu` =>
            logger.info("###########match shangwu, time="+time)
            var k = 0
            for(a <- array){
              userTopicWeight_shangwu.update(k, userTopicWeight_shangwu(k) + a * userOptWeight.get(opttypeArray(pos).toInt).get)
              k = k+1
            }
            cnt_shangwu = cnt_shangwu + 1
          case `xiawu` =>
            logger.info("###########match xiawu, time="+time)
            var k = 0
            for(a <- array){
              userTopicWeight_xiawu.update(k, userTopicWeight_xiawu(k) + a * userOptWeight.get(opttypeArray(pos).toInt).get)
              k = k+1
            }
            cnt_xiawu = cnt_xiawu + 1
          case `wanshang` =>
            logger.info("###########match wanshang, time="+time)
            var k = 0
            for(a <- array){
              userTopicWeight_wanshang.update(k, userTopicWeight_wanshang(k) + a * userOptWeight.get(opttypeArray(pos).toInt).get)
              k = k+1
            }
            cnt_wanshang = cnt_wanshang + 1
        }

        pos = pos + 1
      }

      //更新用户的兴趣主题模型
      if(cnt_lingchen >0) {
        logger.info("#############lingchen size="+cnt_lingchen)
        userTopicWeight_lingchen = updateNewTopicWeight(userTopicWeight_lingchen, oldTopicWeight_lingchen, newsCounts_lingchen, cnt_lingchen)
        mongo.updateTopicByUserIdAndTime(user,lingchen,userTopicWeight_lingchen,(newsCounts_lingchen + cnt_lingchen).toLong)
      }
      if(cnt_shangwu>0) {
        logger.info("#############shangwu size="+cnt_shangwu)
        userTopicWeight_shangwu = updateNewTopicWeight(userTopicWeight_shangwu, oldTopicWeight_shangwu, newsCounts_shangwu, cnt_shangwu)
        mongo.updateTopicByUserIdAndTime(user,shangwu,userTopicWeight_shangwu,(newsCounts_shangwu + cnt_shangwu).toLong)
      }
      if(cnt_xiawu>0) {
        logger.info("#############xiawu size="+cnt_xiawu)
        userTopicWeight_xiawu = updateNewTopicWeight(userTopicWeight_xiawu, oldTopicWeight_xiawu, newsCounts_xiawu, cnt_xiawu)
        mongo.updateTopicByUserIdAndTime(user,xiawu,userTopicWeight_xiawu,(newsCounts_xiawu + cnt_xiawu).toLong)
      }
      if(cnt_wanshang>0) {
        logger.info("#############wanshang size="+cnt_wanshang)
        userTopicWeight_wanshang = updateNewTopicWeight(userTopicWeight_wanshang, oldTopicWeight_wanshang, newsCounts_wanshang, cnt_wanshang)
        mongo.updateTopicByUserIdAndTime(user,wanshang,userTopicWeight_wanshang,(newsCounts_wanshang + cnt_wanshang).toLong)
      }

      //获取用户浏览过的新闻的关键词列表
      val allNews:Array[String] = hbase.loadAllNews(hbasedatasize)
      val brodcastAllNews = sc.broadcast(allNews).value
      val newsTopWordArray = takeTopWordByTFIDF(sc, brodcastAllNews, newsArray, 10)
      logger.info("#############newsTopWordArray size="+newsTopWordArray.size)

      var keywordsMap_lingchen : Map[String, Double] = Map()
      var keywordsMap_shangwu : Map[String, Double] = Map()
      var keywordsMap_xiawu : Map[String, Double] = Map()
      var keywordsMap_wanshang : Map[String, Double] = Map()
      //循环每一篇文章的关键词，用权重乘以行为的权重值
      var newsCnt = 0
      for(topwordArray:Array[(String, Double)] <- newsTopWordArray){
        for((keyword, weight) <- topwordArray){
          val time = checkDatetime(dateArray(newsCnt))
          time match {
            case `lingchen` =>
              val w = weight.toDouble * userOptWeight.get(opttypeArray(newsCnt).toInt).get
              if(keywordsMap_lingchen.contains(keyword)){
                if(w > keywordsMap_lingchen.get(keyword).get.toString.toDouble){
                  keywordsMap_lingchen += (keyword -> w)
                }
              }else{
                keywordsMap_lingchen += (keyword -> w)
              }
            case `shangwu` =>
              val w = weight.toDouble * userOptWeight.get(opttypeArray(newsCnt).toInt).get
              if(keywordsMap_shangwu.contains(keyword)){
                if(w > keywordsMap_shangwu.get(keyword).get.toString.toDouble){
                  keywordsMap_shangwu += (keyword -> w)
                }
              }else{
                keywordsMap_shangwu += (keyword -> w)
              }
            case `xiawu` =>
              val w = weight.toDouble * userOptWeight.get(opttypeArray(newsCnt).toInt).get
              if(keywordsMap_xiawu.contains(keyword)){
                if(w > keywordsMap_xiawu.get(keyword).get.toString.toDouble){
                  keywordsMap_xiawu += (keyword -> w)
                }
              }else{
                keywordsMap_xiawu += (keyword -> w)
              }
            case `wanshang` =>
              val w = weight.toDouble * userOptWeight.get(opttypeArray(newsCnt).toInt).get
              if(keywordsMap_wanshang.contains(keyword)){
                if(w > keywordsMap_wanshang.get(keyword).get.toString.toDouble){
                  keywordsMap_wanshang += (keyword -> w)
                }
              }else{
                keywordsMap_wanshang += (keyword -> w)
              }
          }

        }
        newsCnt= newsCnt +1
      }
      logger.info("#############keywordsMap_lingchen size="+keywordsMap_lingchen.size)
      logger.info("#############keywordsMap_shangwu size="+keywordsMap_shangwu.size)
      logger.info("#############keywordsMap_xiawu size="+keywordsMap_xiawu.size)
      logger.info("#############keywordsMap_wanshang size="+keywordsMap_wanshang.size)

      //更新用户关键词列表
      val keywordArray_lingchen = updateKeywordArray(keywordsMap_lingchen)
      updateKeywordArrayInDatabase(sc, keywordArray_lingchen, user, lingchen)
      val keywordArray_shangwu = updateKeywordArray(keywordsMap_shangwu)
      updateKeywordArrayInDatabase(sc, keywordArray_shangwu, user, shangwu)
      val keywordArray_xiawu = updateKeywordArray(keywordsMap_xiawu)
      updateKeywordArrayInDatabase(sc, keywordArray_xiawu, user, xiawu)
      val keywordArray_wanshang = updateKeywordArray(keywordsMap_wanshang)
      updateKeywordArrayInDatabase(sc, keywordArray_wanshang, user, wanshang)

    }
  }

  /**
   * 取权重在前10的关键词对用户关键词进行更新
   * 如果关键词存在，则将权重相加
   * 如果关键词不存在，则如果权重比用户已有的关键词权重的中间值大，则将新关键词加入
   * @param sc
   * @param keywordArray
   * @param user
   * @param time
   */
  def updateKeywordArrayInDatabase(sc:SparkContext, keywordArray: Array[(String, Double)], user:String, time:String): Unit ={
    val keywordFromNew = sc.parallelize(keywordArray).sortBy(p=>p._2, false).take(10)
    val keywordOld:Map[String, Double] = mongo.getUserKeywordsByUserIdAndTime(sc, user,time)
    var keywordForUpdate:Map[String, Double] = Map()
    for(keyset <- keywordOld.keySet){
      keywordForUpdate += (keyset -> keywordOld.get(keyset).get)
    }
    val middleWeight = getMiddleKeywords(sc, keywordOld)
    for((key, weight) <- keywordFromNew){
      if(keywordForUpdate.contains(key)){
        keywordForUpdate.updated(key, weight + keywordForUpdate.get(key).get.toDouble)
      }else{
        if(weight > middleWeight){
          keywordForUpdate += (key -> weight)
        }
      }
    }
    //更新用户关键词列表
    mongo.updateKeywordByuserIdAndTime(user, time, keywordForUpdate)
  }

  def updateKeywordArray(in:Map[String, Double]): Array[(String, Double)] ={
    val keywordArray = new Array[(String, Double)](in.size)
    var c = 0
    for(keyword <- in.keySet){
      keywordArray(c) = (keyword, in.get(keyword).get.toDouble)
      c = c + 1
    }
    keywordArray
  }


  def updateNewTopicWeight(userTopicWeight:Array[Double], oldTopicWeight:Array[Double], newsCounts:Double, newsVectorSize: Int): Array[Double] ={
    val oldlength = oldTopicWeight.length
    for (m <- 0 to userTopicWeight.length - 1) {
      if(m<oldlength) {
        userTopicWeight.update(m, (userTopicWeight(m) + oldTopicWeight(m)) / (newsCounts + newsVectorSize))
      }else{
        userTopicWeight.update(m, userTopicWeight(m) / (newsCounts + newsVectorSize))
      }
    }
    userTopicWeight
  }

  def backDate(input:Array[Double], newsCounts:Double): Array[Double] ={
    var n = 0
    for (w <- input) {
      input.update(n, input(n) * newsCounts)
      n = n + 1
    }
    input
  }

  /**
   * 判断时间区间
   * @param datetime
   * @return
   */
  def checkDatetime(datetime:String): String = {
    val sf: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    var rtn = ""
    if (datetime != null && datetime.length > 11) {
      val timestr = datetime.substring(11, datetime.length)
      val d = sf.parse(timestr).getTime
      if (isInZone(d, "00:00:00", "05:59:59")) {
        rtn = "凌晨"
      } else if (isInZone(d, "06:00:00", "11:59:59")) {
        rtn = "上午"
      } else if (isInZone(d, "12:00:00", "17:59:59")) {
        rtn = "下午"
      } else if (isInZone(d, "18:00:00", "23:59:59")) {
        rtn = "晚上"
      }
    }
    rtn
  }
  def isInZone(time:Long, start:String, end:String): Boolean ={
    val sf:SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    val starttime = sf.parse(start).getTime
    val endtime = sf.parse(end).getTime
    return time>=starttime && time<=endtime
  }


  def getMiddleKeywords(sc: SparkContext, keyword:Map[String, Double]): Double ={
    if(keyword != null && keyword.size>0) {
      val a = sc.parallelize(keyword.toSeq)
      val b = a.sortBy(p => p._2, false).take(keyword.size/2)
      if(b != null && b.size>0){
        b(0)._2
      }else{
        0.0
      }

    }else{
      0.0
    }
  }


  /**
   * 对于单一新的新闻，产生用户的新闻推荐列表
   * @param sc
   * @param newsSeq
   */
  def genRecommendList(sc: SparkContext, newsSeq: String, doc_id:String, hbasedatasizeforIDF:Long): Array[(String, Array[String])] ={
    //val newsSeq = Seq("/user/root/data/newdoc/docs52018606_6120510368.txt","/user/root/data/newdoc/docs52020522_8152922548.txt","/user/root/data/newdoc/docs52026846_4776750571.txt","/user/root/data/newdoc/docs52027612_8805053799.txt","/user/root/data/newdoc/docs52028617_8152922548.txt","/user/root/data/newdoc/docs52034339_9769652619.txt","/user/root/data/newdoc/docs52043428_1091244780.txt","/user/root/data/newdoc/docs52046199_8818667120.txt","/user/root/data/newdoc/docs52062277_8961011403.txt","/user/root/data/newdoc/docs52069811_5657933100.txt")
    //提取新的新闻的主题

    val newsContentArray = Array{newsSeq}
    val newsVector: Array[(Vector)] = predictTopicForDoc(sc, newsContentArray)
   /* //提取新的新闻的关键词
    val allNews:Array[String] = hbase.loadAllNews(hbasedatasizeforIDF)
    val brodcastAllNews = sc.broadcast(allNews).value
    val newsTopWordArray:Array[Array[(String, Double)]] = takeTopWordByTFIDF(sc, brodcastAllNews, newsSeq, 10)
*/
    //按照用户的顺序循环，计算新闻和用户的主题和关键词的相似度，设定生成新闻列表的阈值，大于阈值的就生成列表
    val userTopicAndKeywords:Array[Map[String, Any]] = mongo.getUserList(sc, "上午")
    val userRecNewsList:Array[(String, Array[String])] = new Array(userTopicAndKeywords.length)
    var usercnt = 0
    for(userMap <- userTopicAndKeywords){
      val userid:String = userMap.get("userid").get.asInstanceOf[String]
      val topic:Array[Double] = userMap.get("topic").get.asInstanceOf[Array[Double]]
      val topicLength = topic.size
      val keyword:Array[String] = userMap.get("keyword").get.asInstanceOf[Array[String]]
      val newsSimList:Array[(String, Double)] = new Array(newsSeq.size)
      for(i <- 0 to newsVector.size -1){
        val news = newsVector(i)
        val newsLength = news.size
        var topicArray = topic
        var newsArray = news.toArray
        if(topicLength > newsLength){
          newsArray = makeEqualLengh(news.toArray, topicLength)
        }else if(topicLength<newsLength){
          topicArray = makeEqualLengh(topic, newsLength)
        }
        val topicRdd = sc.parallelize(Array(topicArray)).map(Vectors.dense(_))
        val newsRdd = sc.parallelize(Array(newsArray)).map(Vectors.dense(_))

        val cosinInput = newsRdd.++(topicRdd)
        val simCosine = calculateCosine(sc, cosinInput)
       /* val keywords = newsTopWordArray(i)
        val inB = new Array[String](keywords.length)
        var j = 0
        for((key, weight) <- keywords){
          inB(j) = key
          j = j + 1
        }
        val simJacaard = calculateJacaardSim(sc, keyword, inB)
        val simU = calculateSimTotal(simCosine, simJacaard, topicWeight, keywordWeight)*/
        newsSimList(i) =(doc_id, simCosine)
      }
      //logger.info("########################newsvalue:"+newsSimList(0)._1)
      val newsList = sc.parallelize(newsSimList).filter(p=>p._2>similarityValue).map(p=>(p._1)).collect()
      if(newsList != null && newsList.size>0) {
        userRecNewsList(usercnt) = (userid, newsList)
        usercnt = usercnt + 1
      }

    }
    userRecNewsList
  }

  /**
   * 对于新的新闻列表，产生用户的新闻推荐列表
   * @param sc
   * @param newsSeq
   */
  def genRecommendList(sc: SparkContext, newsSeq: Array[String], doc_idSeq:Seq[String], doc_typeSeq:Seq[String], hbasedatasizeforIDF:Long): Array[(String, Array[(String,String)])] ={
    //val newsSeq = Seq("/user/root/data/newdoc/docs52018606_6120510368.txt","/user/root/data/newdoc/docs52020522_8152922548.txt","/user/root/data/newdoc/docs52026846_4776750571.txt","/user/root/data/newdoc/docs52027612_8805053799.txt","/user/root/data/newdoc/docs52028617_8152922548.txt","/user/root/data/newdoc/docs52034339_9769652619.txt","/user/root/data/newdoc/docs52043428_1091244780.txt","/user/root/data/newdoc/docs52046199_8818667120.txt","/user/root/data/newdoc/docs52062277_8961011403.txt","/user/root/data/newdoc/docs52069811_5657933100.txt")
    //提取新的新闻的主题
    val newsVector: Array[(Vector)] = predictTopicForDoc(sc, newsSeq)
    //提取新的新闻的关键词
    val allNews:Array[String] = hbase.loadAllNews(hbasedatasizeforIDF)
    val brodcastAllNews = sc.broadcast(allNews).value
    val newsTopWordArray:Array[Array[(String, Double)]] = takeTopWordByTFIDF(sc, brodcastAllNews, newsSeq, 10)

    //按照用户的顺序循环，计算新闻和用户的主题和关键词的相似度，设定生成新闻列表的阈值，大于阈值的就生成列表
    val userTopicAndKeywords:Array[Map[String, Any]] = mongo.getUserList(sc, "上午")
    val userRecNewsList:Array[(String, Array[(String,String)])] = new Array(userTopicAndKeywords.length)
    var usercnt = 0
    for(userMap <- userTopicAndKeywords){
      val userid:String = userMap.get("userid").get.asInstanceOf[String]
      val topic:Array[Double] = userMap.get("topic").get.asInstanceOf[Array[Double]]
      val topicLength = topic.size
      val keyword:Array[String] = userMap.get("keyword").get.asInstanceOf[Array[String]]
      val newsSimList:Array[(String,String, Double)] = new Array(newsSeq.size)
      for(i <- 0 to newsVector.size -1){
        val news = newsVector(i)
        val newsLength = news.size
        var topicArray = topic
        var newsArray = news.toArray
        if(topicLength > newsLength){
          newsArray = makeEqualLengh(news.toArray, topicLength)
        }else if(topicLength<newsLength){
          topicArray = makeEqualLengh(topic, newsLength)
        }
        val topicRdd = sc.parallelize(Array(topicArray)).map(Vectors.dense(_))
        val newsRdd = sc.parallelize(Array(newsArray)).map(Vectors.dense(_))

        val cosinInput = newsRdd.++(topicRdd)
        val simCosine = calculateCosine(sc, cosinInput)
        val keywords = newsTopWordArray(i)
        val inB = new Array[String](keywords.length)
        var j = 0
        for((key, weight) <- keywords){
          inB(j) = key
          j = j + 1
        }
        val simJacaard = calculateJacaardSim(sc, keyword, inB)
        val simU = calculateSimTotal(simCosine, simJacaard, topicWeight, keywordWeight)
        newsSimList(i) =(doc_idSeq(i),doc_typeSeq(i), simU)
      }
      //logger.info("########################newsvalue:"+newsSimList(0)._1)
      val newsList = sc.parallelize(newsSimList).filter(p=>p._3>similarityValue).map(p=>(p._1,p._2)).collect()
      if(newsList != null && newsList.size>0) {
        userRecNewsList(usercnt) = (userid, newsList)
        usercnt = usercnt + 1
      }

    }
    userRecNewsList
  }


  /**
   * 利用训练好的模型和词袋，对任意一篇文章进行LDA分析，生成这篇文章的主题向量
   * @param sc
   * @param newContent
   * @return
   */
  def predictTopicForDoc(sc: SparkContext, newContent: Array[String]): Array[(Vector)] = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val ldaModel = DistributedLDAModel.load(sc, recmodel)
    val wordDic = sc.textFile(dicPath).collect()
    logger.info("#############wordDic="+wordDic)
    val df = sc.parallelize(newContent).toDF("docs")
    val tokenizer = new RegexTokenizer().setInputCol("docs").setOutputCol("rawTokens")
    val dff = tokenizer.transform(df)

    val countVectorizerModel = new CountVectorizerModel("1", wordDic)
    countVectorizerModel.setInputCol("rawTokens").setOutputCol("feature")
    val corpus = countVectorizerModel.transform(dff).select("feature").map { case Row(features: Vector) => features }.zipWithIndex().map(_.swap)

    val docTopic = predict(corpus, ldaModel)
    val topicWeight = docTopic.map { p => p._2 }
    topicWeight

  }

  /**
   * 利用LDA模型预测新文章的主题向量
   * @param documents
   * @param ldaModel
   * @return
   */
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

  /**
   * 补足数组的长度
   * @param array
   * @param length
   * @return
   */
  def makeEqualLengh(array:Array[Double], length:Int): Array[Double] ={
    if(array != null && array.length<length){
      val rtn:Array[Double] = new Array[Double](length)
      var i = 0
      for(a <- array){
        rtn(i) = a
        i = i + 1
      }
      for(j <- i to length -1){
        rtn(j) = 0.0
      }
      rtn
    }else{
      array
    }
  }

  /**
   * 计算两个向量之间的相似度，注意，RowMatrix只能计算列之间的相似度，
   * 所以需要将传进来的向量进行转置变成列向量(调用transpose函数)
   * @param sc
   * @param rows
   * @return
   */
  def calculateCosine(sc: SparkContext, rows : RDD[Vector]): Double ={

    val t_rows = sc.parallelize(transpose(rows.map(p=>(p.toArray)).collect())).map(p=>Vectors.dense(p))
    val mat = new RowMatrix(t_rows)

    val exact = mat.columnSimilarities()

    val simArray = exact.entries.map{case MatrixEntry(i,j,sim)=>
      sim
    }.collect()

    if(simArray != null && simArray.length>0){
      simArray(0).toDouble
    }else{
      0.0
    }

  }

  /**
   * 转置矩阵，为了计算两个列之间的相似度
   * @param m
   * @return
   */
  def transpose( m: Array[Array[Double]] ): Array[Array[Double]] ={
    if(m!=null && m.length>0 ) {
      (for {c <- m(0).indices}
        yield {
          m.map(_(c))
        }
        ).toArray
    }else{
      new Array[Array[Double]](0)
    }
  }

  /**
   * 计算两个数组的Jacaard相似度
   * @param sc
   * @param inA
   * @param inB
   * @return
   */
  def calculateJacaardSim(sc: SparkContext,inA: Array[String], inB: Array[String]): Double ={
    val allString = inA.++(inB)
    val allRdd = sc.parallelize(allString)
    val result = allRdd.map((_,1)).reduceByKey(_+_)
    val inNum = result.filter{case (key, value)=> value>1}.count
    val outNum = result.count
    inNum.toDouble / outNum.toDouble
  }

  /**
   * 根据词频统计获取文章的top词
   * @param sc
   * @param docpath
   * @param top
   * @return
   */
  def takeTopWordForDoc(sc:SparkContext, docpath: String, top: Int): Array[(String, Int)] ={
    val documents = sc.textFile(docpath).flatMap(_.split(" "))
    documents.map((_,1)).reduceByKey(_+_).sortBy(p=> p._2, false).take(top)
  }

  /**
   * 将新文档和所有以前训练过的文档放在一起，进行新文档的TF-IDF计算，
   * 返回根据TF-IDF值排序之后的top个关键字
   * @param sc
   * @param trainingDocPath 文档全集
   * @param docPath 新文档
   * @param top
   * @return
   */
  def takeTopWordByTFIDF(sc:SparkContext, trainingDocPath:Array[String], docPath:Array[String], top: Int ): Array[Array[(String, Double)]] ={
    //val input = trainingDocPath.+:(docPath)
    val input: RDD[Seq[String]] = sc.parallelize(trainingDocPath).map(_.split(" ").toSeq)
    val newDoc = sc.parallelize(docPath).map(_.split(" ").toSeq)
    val documents = newDoc.++(input)
    val hashingTF = new HashingTFWithTerm()
    val tf: RDD[(Vector,mutable.HashMap[Int, String])] = hashingTF.transforms(documents)
    val allWordsArray: Array[mutable.HashMap[Int, String]]= tf.map(p=>p._2).collect

    val ttf = tf.map(p=>p._1)
    val idf = new IDF().fit(ttf)
    val tfidf: RDD[Vector] = idf.transform(ttf)
    val newFileTfIdf = tfidf.take(docPath.length)

    val indices = newFileTfIdf.map{case v=>
      val svv = v.asInstanceOf[SparseVector]
      val indices = svv.indices
      indices
    }

    val values = newFileTfIdf.map{case v=>
      val svv = v.asInstanceOf[SparseVector]
      val values = svv.values
      values
    }
    val rtn = new Array[Array[(String, Double)]](docPath.length)
    var j = 0
    for(i<- 0 to indices.length-1) {
      val indicesRdd = sc.parallelize(indices(i)).zipWithIndex().map(_.swap)
      val valuesRdd = sc.parallelize(values(i)).zipWithIndex().map(_.swap)
      val top10Values = valuesRdd.sortBy(p => p._2, false).take(top)

      val newFileAllWord: mutable.HashMap[Int, String] = allWordsArray(0)
      val tmp = new Array[(String, Double)](top)
      var cnt = 0
      for ((id, weight) <- top10Values) {
        val pos = indicesRdd.filter { case (ids, aid) => ids == id }.map(p => p._2).take(1)(0)
        val term = newFileAllWord.get(pos)
        if(term != None) {
          tmp(cnt) = (term.get.toString, weight.toDouble)
          cnt = cnt + 1
        }
      }
      rtn(j) = tmp
      j = j +1
    }

    rtn
  }


  /**
   * 计算最终的新闻的主题模型和用户兴趣模型的相似度
   * @param simT 主题相似度
   * @param simK 关键字相似度
   * @param sita 主题相似度的比例
   * @param beta 关键字相似度的比例
   * @return
   */
  def calculateSimTotal(simT:Double, simK:Double, sita:Double, beta:Double): Double ={
    (simT * sita + simK * beta) / Math.sqrt(sita * sita + beta * beta)
  }

  /**
   * 对输入的所有文章，生成词频统计向量
   * @param sc
   * @param folder
   * @param vocabSize
   * @param stopwordFile
   * @return
   */
  def preprocess(sc: SparkContext, folder:Array[String], vocabSize: Int, stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long) = {

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    // Get dataset of document texts
    // One document per line in each text file. If the input consists of many small files,
    // this can result in a large number of small partitions, which can degrade performance.
    // In this case, consider using coalesce() to create fewer, larger partitions.
    val df = sc.parallelize(folder).toDF("docs")
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

  /**
   * 获取所有训练数据新闻的主题向量
   * @param sc
   * @param topicNumber
   * @return
   */
  def getTrainingToppicForDoc(sc: SparkContext, topicNumber:Integer): RDD[(Long, Array[Double])] ={
    val ldaModel = DistributedLDAModel.load(sc, recmodel)
    val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
    val topTopics = distLDAModel.topTopicsPerDocument(topicNumber)
    val topicWeight = topTopics.map{case (newsId,topicList,weightList)=>
      val t = topicNumber
      val array = new Array[Double](t)
      var i = 0
      for(p <-topicList){
        array(p) = weightList(i)
        i += 1
      }

      (newsId, array)
    }
    topicWeight
  }

}
