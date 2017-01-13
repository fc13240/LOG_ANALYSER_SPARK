package com.udbac.sparklog

import java.net.URLDecoder

import eu.bitwalker.useragentutils.UserAgent
import com.udbac.constant.SDCLogConstants
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map
import scala.com.udbac.util.{IPSeekerExt, QueryProperties, SplitValueBuilder}
/**
  * Created by root on 2017/1/12.
  */
object LogAnalyser {
 val ipSeekerExt = new IPSeekerExt

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("LogAnalyser")
    val sc = new SparkContext(conf)
    val lineRDD = sc.textFile(args(0))
    lineRDD.map(line => log(line,QueryProperties.query())).saveAsTextFile(args(1))
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
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_DATE_TIME, lineSplits(0) + lineSplits(1))
      logMap.put(SDCLogConstants.LOG_COLUMN_CS_HOST, lineSplits(4))
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CSMETHOD, lineSplits(5))
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CSURISTEM, lineSplits(6))
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_SCSTATUS, lineSplits(8))
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_DCSID, lineSplits(14))
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
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_OS_NAME,userAgent.getOperatingSystem.getName)
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,userAgent.getBrowser.getName)
    }
  }

  def handleIP(logMap: Map[String, String], ip: String): Unit ={
    if (!ip.equals(null) && ip.length > 8){
      val info = ipSeekerExt.analyticIp(ip)
      if (null != info) {
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_COUNTRY, info.getCountry)
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_PROVINCE, info.getProvince)
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CITY, info.getCity)
      }
    }
  }
}
