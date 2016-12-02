package com.recsystem.tmp

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by lee on 2016/5/17.
 */
object TestHbase {
  val conf = new SparkConf().setAppName("RecMain")
  val sc = new SparkContext(conf)
  def testhbase(): Unit ={

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "sparkmaster")

    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)

    val scores = TableName.valueOf("news")
    val table = conn.getTable(scores)

    val g = new Get("news0001".getBytes())
    val result = table.get(g)
    val value = Bytes.toString(result.getValue("words".getBytes,"".getBytes))
    println("GET news :"+value)
    /*

        conf.set(TableInputFormat.INPUT_TABLE, "scores")
        val scan = new Scan()
        scan.setFilter(new SingleColumnValueFilter("basic".getBytes,"age".getBytes,
          CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(18)))
        conf.set(TableInputFormat.SCAN, convertScanToString(scan))

        val usersRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
          classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
          classOf[org.apache.hadoop.hbase.client.Result])

        val count = usersRDD.count()
        usersRDD.foreach{ case (_,result) =>
          val key = Bytes.toInt(result.getRow)
          val math = Bytes.toString(result.getValue("course".getBytes,"math".getBytes))
          val china = Bytes.toString(result.getValue("course".getBytes,"china".getBytes))
          println("Row key:"+key+" math:"+math+" china:"+china)
        }
    */

  }
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

}
