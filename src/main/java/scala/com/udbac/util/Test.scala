package scala.com.udbac.util


import java.io.IOException
import java.util
import java.util.Comparator

import com.udbac.constant.LogConstants

import scala.collection.mutable.Map
import scala.io.Source

/**
  * Created by root on 2017/1/19.
  */
object Test {
  val mapSegs = Map[Integer, String]()
  val mapArea = Map[String, Array[String]]()
  val sortedList = new util.ArrayList[Integer]()
  @throws[IOException]
  def readCsv() {
    val segsPath = "udbacIPtransSegs.csv"
    val segSources = Source.fromFile(segsPath)
    for (oneline <- segSources.getLines()) {
     val strings = oneline.split(LogConstants.IPCSV_SEPARTIOR)
      val startIPInt = IPv4Util.ipToInt(strings(0))
      mapSegs.put(startIPInt,strings(2))
      sortedList.add(startIPInt)
      sortedList.sort(new Comparator[Integer]() {
        def compare(integer: Integer, anotherInteger: Integer): Int = integer.compareTo(anotherInteger)
      })
    }

    val areaPath = "udbacIPtransArea.csv"
    val areaSources = Source.fromFile(areaPath)
    for (oneline <- areaSources.getLines()) {
      val strings = oneline.split(LogConstants.IPCSV_SEPARTIOR)
      mapArea.put(strings(2), strings)
    }
  }

  @throws[IOException]
  def getArea(logIP: String): Array[String] = mapArea.get(getIPcode(logIP))

  @throws[IOException]
  def getIPcode(logIP: String): Option[String] = {

    val index: Integer = SeaIP.searIP(sortedList, IPv4Util.ipToInt(logIP))
    mapSegs.get(sortedList.get(index))
  }
}
