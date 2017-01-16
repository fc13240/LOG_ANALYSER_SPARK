package com.udbac.sparklog

import java.net.URLDecoder

import eu.bitwalker.useragentutils.UserAgent
import com.udbac.constant.LogConstants
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map
import scala.com.udbac.util.{IPv4Handler, QueryProperties, SplitValueBuilder, TimeUtil}
/**
  * Created by root on 2017/1/12.
  */
object LogAnalyser {
 val ipv4Handler = new IPv4Handler

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("LogAnalyser")
    val sc = new SparkContext(conf)
    val lineRDD = sc.textFile(args(0))
    lineRDD.map(line => log(line,QueryProperties.query())).saveAsTextFile(args(1))
//    lineRDD.map(line => logParser(line)).saveAsTextFile(args(1))
  }
  def log(line: String, keys: Array[String]): SplitValueBuilder ={
      val svb = new SplitValueBuilder()
//    val logMap = logParser(line)
//    val x = logMap.keySet
    for (key <- keys){
      for ((x,y) <- logParser(line)){
          if (x == key)
          svb.add(y)
        }
//        svb.add(logMap.get(key))

      }
      svb
  }

  def logParser(lineStr: String): Map[String, String] = {
    val logMap = Map[String, String]()
    val lineSplits = lineStr.split(" ")
    if(lineSplits.length==15){
//      logMap.put(LogConstants.LOG_COLUMN_DATETIME, TimeUtil.handleTime(lineSplits(0) + " " + lineSplits(1)))
      //TimeUtil.handleTime(lineSplits(0) + " " + lineSplits(1))  加8小时
      logMap.put(LogConstants.LOG_COLUMN_DATETIME, lineSplits(0) + " " + lineSplits(1))
      logMap.put(LogConstants.LOG_COLUMN_HOST, lineSplits(4))
      logMap.put(LogConstants.LOG_COLUMN_METHOD, lineSplits(5))
      logMap.put(LogConstants.LOG_COLUMN_URISTEM, lineSplits(6))
      logMap.put(LogConstants.LOG_COLUMN_STATUS, lineSplits(8))
      logMap.put(LogConstants.LOG_COLUMN_DCSID, lineSplits(14))
      handleQuery(logMap, lineSplits(7))
      handleUA(logMap, lineSplits(11))
      handleIP(logMap, lineSplits(2))
    }
    logMap
  }

  def handleQuery(logMap: Map[String, String], queryStr: String): Unit = {
    if (queryStr.length > 10) {
      val querySplits = queryStr.split("&")
      for (querySplit <- querySplits) {
        val items = querySplit.split("=")
        if (items.length == 2) {
          items(1) = URLDecoder.decode(items(1), "UTF-8")
          logMap.put(items(0),items(1))
        }
      }
    }
  }

  def handleUA(logMap: Map[String, String], uaStr: String): Unit = {
    if (!uaStr.equals(null)) {
      val userAgent = UserAgent.parseUserAgentString(uaStr)
      logMap.put(LogConstants.UA_OS_NAME,userAgent.getOperatingSystem.getName)
      logMap.put(LogConstants.UA_BROWSER_NAME,userAgent.getBrowser.getName)
    }
  }

  def handleIP(logMap: Map[String, String], ip: String): Unit ={
    if (!ip.equals(null) && ip.length > 8){
      val info = IPv4Handler.getIPcode(ip)
      val area = IPv4Handler.getArea(ip)
      if (null != info) {
        logMap.put(LogConstants.LOG_COLUMN_IPCODE,info)
        logMap.put(LogConstants.LOG_COLUMN_IP,area(0)+area(1))
      }
    }
  }
}
