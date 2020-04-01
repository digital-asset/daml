// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import com.daml.lf.archive.DarManifestReader
import com.daml.lf.archive.DarReader
import com.daml.lf.codegen.backend.Backend
import com.daml.lf.codegen.backend.java.JavaBackend
import com.daml.lf.codegen.conf.Conf
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.iface.reader.{Errors, InterfaceReader}
import com.daml.lf.iface.{Type => _, _}
import com.daml.daml_lf_dev.DamlLf
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

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

    conf.darFiles.foreach {
      case (path, _) =>
        assertInputFileExists(path)
        assertInputFileIsReadable(path)
    }
    checkAndCreateOutputDir(conf.outputDirectory)

    val executor = Executors.newFixedThreadPool(
      Runtime.getRuntime.availableProcessors(),
      new ThreadFactory {
        val n = new AtomicInteger(0)
        override def newThread(r: Runnable): Thread = {
          val t = new Thread(r)
          t.setDaemon(true)
          t.setName(s"java-codegen-${n.getAndIncrement}")
          t
        }
      }
    )
    val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    val (interfaces, pkgPrefixes) = collectDamlLfInterfaces(conf)
    generateCode(interfaces, conf, pkgPrefixes)(ec)
    val _ = executor.shutdownNow()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private[codegen] def collectDamlLfInterfaces(
      conf: Conf): (Seq[Interface], Map[PackageId, String]) = {
    val interfacesAndPrefixes = conf.darFiles.toList.flatMap {
      case (path, pkgPrefix) =>
        val file = path.toFile
        // Explicitly calling `get` to bubble up any exception when reading the dar
        val dar = ArchiveReader.readArchiveFromFile(file).get
        dar.all.map { archive =>
          val (errors, interface) = InterfaceReader.readInterface(archive)
          if (!errors.equals(Errors.zeroErrors)) {
            throw new RuntimeException(
              InterfaceReader.InterfaceReaderError.treeReport(errors).toString)
          }
          logger.trace(s"DAML-LF Archive decoded, packageId '${interface.packageId}'")
          (interface, interface.packageId -> pkgPrefix)
        }
    }

    val interfaces = interfacesAndPrefixes.map(_._1)
    val prefixes = interfacesAndPrefixes.collect {
      case (_, (key, Some(value))) => (key, value)
    }.toMap
    (interfaces, prefixes)
  }

  private[CodeGenRunner] def generateFile(
      outputFile: Path,
      dataTypes: ImmArray[DefDataType.FWT],
      templates: ImmArray[DefTemplate.FWT]): Unit = {
    logger.warn(
      s"Started writing file '$outputFile' with data types ${dataTypes.toString} and templates ${templates.toString}")
    val _ = Files.createDirectories(outputFile.getParent)
    if (!Files.exists(outputFile)) {
      val _ = Files.createFile(outputFile)
    }
    val os = Files.newOutputStream(
      outputFile,
      StandardOpenOption.WRITE,
      StandardOpenOption.TRUNCATE_EXISTING)
    os.close()
    logger.warn(s"Finish writing file '$outputFile'")
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private[CodeGenRunner] def generateCode(
      interfaces: Seq[Interface],
      conf: Conf,
      pkgPrefixes: Map[PackageId, String])(implicit ec: ExecutionContext): Unit = {
    logger.info(
      s"Start processing packageIds '${interfaces.map(_.packageId).mkString(", ")}' in directory '${conf.outputDirectory}'")

    // TODO (mp): pre-processing and escaping
    val preprocessingFuture: Future[InterfaceTrees] =
      backend.preprocess(interfaces, conf, pkgPrefixes)

    val future: Future[Unit] = {
      for {
        preprocessedInterfaceTrees <- preprocessingFuture
        _ <- Future.traverse(preprocessedInterfaceTrees.interfaceTrees)(
          processInterfaceTree(_, conf, pkgPrefixes))
      } yield ()
    }

    // TODO (mp): make the timeout configurable
    val _ = Await.result(future, Duration.create(10l, TimeUnit.MINUTES))
    logger.info(s"Finish processing packageIds ''${interfaces.map(_.packageId).mkString(", ")}''")
  }

  // TODO (#584): Make Java Codegen Backend configurable
  private[codegen] val backend: Backend = JavaBackend

  private[CodeGenRunner] def processInterfaceTree(
      interfaceTree: InterfaceTree,
      conf: Conf,
      packagePrefixes: Map[PackageId, String])(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info(s"Start processing packageId '${interfaceTree.interface.packageId}'")
    for {
      _ <- interfaceTree.process(backend.process(_, conf, packagePrefixes))
    } yield {
      logger.info(s"Stop processing packageId '${interfaceTree.interface.packageId}'")
    }
  }

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
        s"Output directory '$outputPath' exists but it is not a directory")
    }
  }

  object ArchiveReader
      extends DarReader[DamlLf.Archive](
        DarManifestReader.dalfNames,
        { case (_, is) => Try(DamlLf.Archive.parseFrom(is)) }
      )
}
