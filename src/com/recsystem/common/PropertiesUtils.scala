package com.recsystem.common

import java.util.Properties
import com.stratio.datasource.util.using
import java.io.InputStream
import java.util.Properties
import scala.collection.JavaConversions.propertiesAsScalaMap


/**
 * Created by lee on 2016/5/20.
 */
object PropertiesUtils{
  val prop = new Properties()

  def getPropertiesValueByKey(key:String):String={
    using(this.getClass.getClassLoader.getResourceAsStream("config.properties")) { source =>
    {
      prop.load(source)
    }
    }
    prop.getProperty(key)
  }
}
