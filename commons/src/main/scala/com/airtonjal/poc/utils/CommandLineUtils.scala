package com.airtonjal.poc.utils

import java.io.{FileFilter, File}

import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.commons.logging.LogFactory

/**
 * Utilities object to parse command line arguments
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object CommandLineUtils {

  private val log = LogFactory.getLog(getClass())

  def getFiles (path: String) : Array[File] = {
    log.info("Path provided " + path)

    var pathFile: File = null
    var evaluatedPath: String = path
    var filter: FileFilter = null

    if (path.contains("*")) {
      // Wildcard supplied
      filter = new WildcardFileFilter(path.substring(path.lastIndexOf(File.separatorChar) + 1, path.length()))
      evaluatedPath = path.substring(0, path.lastIndexOf(File.separatorChar))
    }

    pathFile = new File(evaluatedPath)

    if (!pathFile.exists())
      throw new IllegalArgumentException("\'path\' parameter should either be an existent file or directory")

    if (pathFile.isFile()) Array { pathFile }
    else                   pathFile.listFiles(filter)
  }

}
