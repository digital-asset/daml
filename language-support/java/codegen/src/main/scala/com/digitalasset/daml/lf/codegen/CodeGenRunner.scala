// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import java.io.{BufferedInputStream, InputStream}
import java.nio.file.{FileSystems, Files, Path, StandardOpenOption}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}
import java.util.jar.Manifest

import com.digitalasset.daml.lf.codegen.conf.Conf
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.iface.reader.{Interface, InterfaceReader}
import com.digitalasset.daml.lf.iface.{Type => _, _}
import com.digitalasset.daml_lf.DamlLf
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

private[codegen] object CodeGenRunner extends StrictLogging {

  def run(conf: Conf): Unit = {

    LoggerFactory
      .getLogger(Logger.ROOT_LOGGER_NAME)
      .asInstanceOf[ch.qos.logback.classic.Logger]
      .setLevel(conf.verbosity)
    LoggerFactory
      .getLogger("com.digitalasset.daml.lf.codegen.backend.java.inner")
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
    val interfacesAndPrefixes = conf.darFiles.flatMap {
      case (path, pkgPrefix) =>
        if (path.getFileName().toString.toLowerCase().endsWith(".dar")) {
          logger.trace(s"Opening DAR '$path'")
          ResourceManagement.withResources(FileSystems.newFileSystem(path, null)) { zipFileSystem =>
            {
              val manifestPath = zipFileSystem.getPath("META-INF/MANIFEST.MF")
              val locations = ResourceManagement.withResources(Files.newInputStream(manifestPath)) {
                inputStream =>
                  getDamlLfPathsFromManifest(inputStream)
              }
              locations
                .map(path => {
                  logger.trace(s"Opening DamlLf file '$path'")
                  ResourceManagement.withResources(
                    Files.newInputStream(zipFileSystem.getPath(path))) { fileInputStream =>
                    readDamlLfInterface(fileInputStream, pkgPrefix)
                  }
                })
                .toIndexedSeq
            }
          }

        } else {
          logger.trace(s"Opening DamlLf file '$path'")
          List(ResourceManagement.withResources(Files.newInputStream(path)) { fileInputStream =>
            readDamlLfInterface(fileInputStream, pkgPrefix)
          })
        }
    }(collection.breakOut)

    (interfacesAndPrefixes.map(_._1), interfacesAndPrefixes.collect {
      case (_, (key, Some(value))) => (key, value)
    }.toMap)
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

  private[CodeGenRunner] def generateCode(
      interfaces: Seq[Interface],
      conf: Conf,
      pkgPrefixes: Map[PackageId, String])(implicit ec: ExecutionContext): Unit = {
    logger.warn(
      s"Start processing packageIds '${interfaces.map(_.packageId.underlyingString).mkString(", ")}' in directory '${conf.outputDirectory}'")

    // TODO (mp): pre-processing and escaping
    val preprocessingFuture: Future[InterfaceTrees] =
      conf.backend.preprocess(interfaces, conf, pkgPrefixes)

    val future: Future[Unit] = {
      for {
        preprocessedInterfaceTrees <- preprocessingFuture
        _ <- Future.traverse(preprocessedInterfaceTrees.interfaceTrees)(
          processInterfaceTree(_, conf, pkgPrefixes))
      } yield ()
    }

    // TODO (mp): make the timeout configurable
    val _ = Await.result(future, Duration.create(10l, TimeUnit.MINUTES))
    logger.warn(
      s"Finish processing packageIds ''${interfaces.map(_.packageId.underlyingString).mkString(", ")}''")
  }

  private[CodeGenRunner] def processInterfaceTree(
      interfaceTree: InterfaceTree,
      conf: Conf,
      packagePrefixes: Map[PackageId, String])(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info(
      s"Start processing packageId '${interfaceTree.interface.packageId.underlyingString}'")
    for {
      _ <- interfaceTree.process(conf.backend.process(_, conf, packagePrefixes))
    } yield {
      logger.info(
        s"Stop processing packageId '${interfaceTree.interface.packageId.underlyingString}'")
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

  private[CodeGenRunner] def readDamlLfInterface(
      inputStream: InputStream,
      pkgPrefix: Option[String]) = {
    val buffered = new BufferedInputStream(inputStream)
    val archive = DamlLf.Archive.parseFrom(buffered)
    // TODO (mp): errors
    val (_, interface) = InterfaceReader.readInterface(archive)
    logger.trace(s"DAML-LF Archive decoded, packageId '${interface.packageId.underlyingString}'")
    (interface, interface.packageId -> pkgPrefix)
  }

  private[CodeGenRunner] def getDamlLfPathsFromManifest(inputStream: InputStream) = {
    val manifest = new Manifest(inputStream).getMainAttributes
    (
      Option(manifest.getValue("Format")),
      Option(manifest.getValue("Location")),
      Option(manifest.getValue("Dalfs"))) match {
      case (Some("daml-lf"), None, Some(dalfList)) => dalfList.split(',').map(_.trim).toList
      case (Some("daml-lf"), Some(location), None) => List(location)
      case _ => throw new IllegalArgumentException("Invalid DAR Manifest")
    }
  }
}
