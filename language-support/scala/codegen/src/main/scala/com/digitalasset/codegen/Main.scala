// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import java.io.File
import java.nio.file.Path

import ch.qos.logback.classic.Level
import com.digitalasset.daml.lf.codegen.conf.Conf
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.{Logger, LoggerFactory}
import scalaz.Cord

import scala.collection.breakOut

object Main extends StrictLogging {

  private val codegenId = "Scala Codegen"

  @deprecated("Use codegen font-end: com.digitalasset.codegen.CodegenMain.main", "0.13.23")
  def main(args: Array[String]): Unit =
    Conf.parse(args) match {
      case Some(conf) =>
        generateCode(conf)
      case None =>
        throw new IllegalArgumentException(
          s"Invalid ${codegenId: String} command line arguments: ${args.mkString(" "): String}")
    }

  def generateCode(conf: Conf): Unit = conf match {
    case Conf(darMap, outputDir, decoderPkgAndClass, verbosity, roots) =>
      setGlobalLogLevel(verbosity)
      logUnsupportedEventDecoderOverride(decoderPkgAndClass)
      val (dars, packageName) = darsAndOnePackageName(darMap)
      CodeGen.generateCode(dars, packageName, outputDir.toFile, CodeGen.Novel, roots)
  }

  private def setGlobalLogLevel(verbosity: Level): Unit = {
    LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) match {
      case a: ch.qos.logback.classic.Logger =>
        a.setLevel(verbosity)
        logger.info(s"${codegenId: String} verbosity: ${verbosity.toString}")
      case _ =>
        logger.warn(s"${codegenId: String} cannot set requested verbosity: ${verbosity.toString}")
    }
  }

  private def logUnsupportedEventDecoderOverride(mapping: Option[(String, String)]): Unit =
    mapping.foreach {
      case (a, b) =>
        logger.warn(
          s"${codegenId: String} does not allow overriding Event Decoder, skipping: ${a: String} -> ${b: String}")
    }

  private def darsAndOnePackageName(darMap: Map[Path, Option[String]]): (List[File], String) = {
    val dars: List[File] = darMap.keys.map(_.toFile)(breakOut)
    val uniquePackageNames: Set[String] = darMap.values.collect { case Some(x) => x }(breakOut)
    uniquePackageNames.toSeq match {
      case Seq(packageName) =>
        (dars, packageName)
      case _ =>
        throw new IllegalStateException(
          s"${codegenId: String} expects all dars mapped to the same package name, " +
            s"requested: ${format(darMap): String}")
    }
  }

  private def format(map: Map[Path, Option[String]]): String = {
    val cord = map.foldLeft(Cord("{")) { (str, kv) =>
      str ++ kv._1.toFile.getAbsolutePath ++ "->" ++ kv._2.toString ++ ","
    }
    (cord ++ "}").toString
  }
}
