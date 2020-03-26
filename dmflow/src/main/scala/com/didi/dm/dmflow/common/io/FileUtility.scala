package com.didi.dm.dmflow.common.io

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.google.common.base.Strings
import org.apache.spark.internal.Logging

import scala.io.{Codec, Source}


object FileUtility extends Logging {

  def exists(filePath: String): Boolean = Files.exists(Paths.get(filePath))

  def mkdir(path: String): Boolean = {
    require(!Strings.isNullOrEmpty(path), s"mkdir but input path is null or empty: $path")

    if (!Files.exists(Paths.get(path))) {
      Files.createDirectories(Paths.get(path))
      true
    } else {
      logWarning(s"mkdir but already exists: $path")
      false
    }
  }

  def write(dumpPath: String, text: String, overwrite: Boolean): Boolean = {
    require(!Strings.isNullOrEmpty(dumpPath), s"write local text file but dumpPath is null or empty: $dumpPath")

    if (exists(dumpPath)) {
      if (overwrite) {
        logWarning(s"write local text file by overwrite: $dumpPath")
        Files.delete(Paths.get(dumpPath))
      } else {
        throw new RuntimeException(s"write local text file but it already exists: $dumpPath")
      }
    }

    try {
      Files.write(Paths.get(dumpPath), text.getBytes(StandardCharsets.UTF_8))
    } catch {
      case ex: Exception =>
        logError(s"write local text file failed! dump=$dumpPath msg=${ex.getMessage}", ex)
        return false
    }

    true
  }

  def readLines(filePath: String): String = {

    val src = Source.fromFile(filePath, Codec.UTF8.name)

    try {
      src.getLines.toSeq.mkString("\n")
    }
    finally src match {
      case b: scala.io.BufferedSource => b.close
    }

  }

  @throws(classOf[IllegalArgumentException])
  def readLines(filePath: String,
                placeholderMap: Map[String, String],
                isCheck: Boolean = true,
                checkWhitelist: List[String] = List[String](),
                verbose: Boolean = false): String = {

    require(!Strings.isNullOrEmpty(filePath), "filePath is null or empty")
    require(Files.exists(Paths.get(filePath)), s"filePath is not exists: $filePath")
    require(placeholderMap.nonEmpty, "placeholder map is null or empty")

    if (verbose) logInfo(s"load text file with placeholders：$filePath ...")

    val rawLines = readLines(filePath)
    val lines = if (placeholderMap.nonEmpty) {
      Templates.format(rawLines, placeholderMap, isCheck, checkWhitelist)
    } else {
      rawLines
    }

    if (verbose) logInfo(lines)

    lines
  }

}
