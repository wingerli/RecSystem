package com.recsystem.common

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{ResultScanner, Scan, ConnectionFactory, Get}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by lee on 2016/5/17.
 */
class HbaseUtils {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.property.clientPort", PropertiesUtils.getPropertiesValueByKey("hbase_port"))
  conf.set("hbase.zookeeper.quorum", PropertiesUtils.getPropertiesValueByKey("hbase_addr"))
  //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
  val conn = ConnectionFactory.createConnection(conf)

  def readNews(key: String): String ={
    val table = conn.getTable(TableName.valueOf("news"))
    val g = new Get(key.getBytes())
    val result = table.get(g)
    Bytes.toString(result.getValue("content".getBytes,"words".getBytes))
  }
  def loadAllNews(hbasesize: Long):Array[String]={
    val table = conn.getTable(TableName.valueOf("news"))
    val scan = new Scan()
    scan.addColumn("content".getBytes,"words".getBytes)
    val result: ResultScanner = table.getScanner(scan)
    val rtn  = ArrayBuffer[String]()
    try{
      var i = 0
      var r = result.next()
      if(hbasesize>0) {
        while (r != null && i <= hbasesize) {
          val words = Bytes.toString(r.getValue("content".getBytes, "words".getBytes))
          rtn += words
          r = result.next()
          i = i + 1
        }
      }else{
        while (r != null) {
          val words = Bytes.toString(r.getValue("content".getBytes, "words".getBytes))
          rtn += words
          r = result.next()
        }
      }
    }finally {
      //确保scanner关闭
      result.close()
    }
    rtn.toArray
  }
}
