// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory}

import com.daml.lf.codegen.backend.Backend
import com.daml.lf.codegen.backend.java.JavaBackend
import com.daml.lf.codegen.conf.Conf
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.{LoggerFactory, Logger}

import scala.concurrent.{ExecutionContextExecutorService, ExecutionContext}

object CodeGenRunner extends StrictLogging {

  def run(conf: Conf): Unit = {

    LoggerFactory
      .getLogger(Logger.ROOT_LOGGER_NAME)
      .asInstanceOf[ch.qos.logback.classic.Logger]
      .setLevel(conf.verbosity)
    LoggerFactory
      .getLogger("com.daml.lf.codegen.backend.java.inner")
      .asInstanceOf[ch.qos.logback.classic.Logger]
      .setLevel(conf.verbosity)

    conf.darFiles.foreach { case (path, _) =>
      assertInputFileExists(path)
      assertInputFileIsReadable(path)
    }
    checkAndCreateOutputDir(conf.outputDirectory)

    val codegen = CodeGen.configure(backend, conf)

    val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(
        Executors.newFixedThreadPool(
          Runtime.getRuntime.availableProcessors(),
          new ThreadFactory {
            val n = new AtomicInteger(0)
            override def newThread(r: Runnable): Thread = {
              val t = new Thread(r)
              t.setDaemon(true)
              t.setName(s"java-codegen-${n.getAndIncrement}")
              t
            }
          },
        )
      )

    codegen.runWith(executionContext)

    val _ = executionContext.shutdownNow()
  }

  // TODO (#584): Make Java Codegen Backend configurable
  private[codegen] val backend: Backend = JavaBackend

  private[CodeGenRunner] def assertInputFileExists(filePath: Path): Unit = {
    logger.trace(s"Checking that the file '$filePath' exists")
    if (Files.notExists(filePath)) {
      throw new IllegalArgumentException(s"Input file '$filePath' doesn't exist")
    }
  }

  private[CodeGenRunner] def assertInputFileIsReadable(filePath: Path): Unit = {
    logger.trace(s"Checking that the file '$filePath' is readable")
    if (!Files.isReadable(filePath)) {
      throw new IllegalArgumentException(s"Input file '$filePath' is not readable")
    }
  }

  private[CodeGenRunner] def checkAndCreateOutputDir(outputPath: Path): Unit = {
    val exists = Files.exists(outputPath)
    if (!exists) {
      logger.trace(s"Output directory '$outputPath' does not exists, creating it")
      val _ = Files.createDirectories(outputPath)
    } else if (!Files.isDirectory(outputPath)) {
      throw new IllegalArgumentException(
        s"Output directory '$outputPath' exists but it is not a directory"
      )
    }
  }
}
