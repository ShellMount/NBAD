package cn.net.yunshan.Nbad

import java.io.FileInputStream
import java.util.Properties

class ConfigFile(path: String) {
  @transient lazy val logger = org.apache.log4j.LogManager.getLogger(getClass)
  var properties = new Properties()
  properties.load(new FileInputStream(path))

  def getItemAsInt(key: String): Int = {
    val value = getItem(key)
    if (value != null) value.toInt else 0
  }

  def getItem(key: String): String = {
    val value = properties.getProperty(key)
    if (value == null) {
      logger.warn(s"Value of $key is null, use default instead")
    }
    value
  }

  def getItemAsString(key: String): String = {
    val value = getItem(key)
    if (value != null) value else ""
  }

  def getItemAsArray(key: String): Array[String] = {
    var value = getItem(key)
    if (value == null) value = ""
    value.split(",")
  }
}
