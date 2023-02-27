package com.virtuslab.test

object ResourceUtils {

  def getResourcePaths(resources: String*): Seq[String] =
    resources.map(fileName => getClass.getResource(s"/$fileName").getPath)

}
