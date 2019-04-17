// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import java.io.File
import java.nio.file.Path

import ch.qos.logback.classic.Level
import com.digitalasset.daml.lf.codegen.conf.Conf
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.{Logger, LoggerFactory}

object Main extends StrictLogging {

  private val codegenId = "Scala Codegen"

  def main(args: Array[String]): Unit =
    Conf.parse(args) match {
      case Some(Conf(darMap, outputDir, decoderPkgAndClass, verbosity)) =>
        setGlobalLogLevel(verbosity)
        logUnsupportedEventDecoderOverride(decoderPkgAndClass)
        val (dars, packageName) = darsAndOnePackageName(darMap)
        CodeGen.generateCode(dars, packageName, outputDir.toFile, CodeGen.Novel)
      case None =>
        throw new IllegalArgumentException(
          s"Invalid $codegenId command line arguments: ${args.mkString(" ")}")
    }

  private def setGlobalLogLevel(verbosity: Level): Unit = {
    LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) match {
      case a: ch.qos.logback.classic.Logger =>
        a.setLevel(verbosity)
        logger.info(s"$codegenId verbosity: $verbosity")
      case _ =>
        logger.warn(s"$codegenId cannot set requested verbosity: $verbosity")
    }
  }

  private def logUnsupportedEventDecoderOverride(mapping: Option[(String, String)]): Unit =
    mapping.foreach { decoderMapping =>
      logger.warn(s"$codegenId does not allow overriding Event Decoder, requested: $decoderMapping")
    }

  private def darsAndOnePackageName(darMap: Map[Path, Option[String]]): (List[File], String) = {
    val empty: (List[File], Set[String]) = (List.empty, Set.empty)
    val (dars, packageNames) =
      darMap.foldRight(empty)((a, as) => (a._1.toFile :: as._1, as._2 + a._2.getOrElse("daml")))
    packageNames.toSeq match {
      case Seq(packageName) =>
        (dars, packageName)
      case _ =>
        throw new IllegalStateException(
          s"$codegenId expects all dars mapped to the same package name, requested: $darMap")
    }
  }
}
