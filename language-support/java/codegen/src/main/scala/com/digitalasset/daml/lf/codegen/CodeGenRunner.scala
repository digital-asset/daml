// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import com.daml.lf.archive.DarParser
import com.daml.lf.codegen.backend.Backend
import com.daml.lf.codegen.backend.java.JavaBackend
import com.daml.lf.codegen.conf.{Conf, PackageReference}
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.iface.reader.{Errors, InterfaceReader}
import com.daml.lf.iface.{Type => _, _}
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

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
      },
    )
    val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    val (interfaces, pkgPrefixes) = collectDamlLfInterfaces(conf)
    generateCode(interfaces, conf, pkgPrefixes)(ec)
    val _ = executor.shutdownNow()
  }

  private[codegen] def collectDamlLfInterfaces(
      conf: Conf
  ): (Seq[Interface], Map[PackageId, String]) = {
    val interfacesAndPrefixes = conf.darFiles.toList.flatMap { case (path, pkgPrefix) =>
      val file = path.toFile
      // Explicitly calling `get` to bubble up any exception when reading the dar
      val dar = DarParser.assertReadArchiveFromFile(file)
      dar.all.map { archive =>
        val (errors, interface) = InterfaceReader.readInterface(archive)
        if (!errors.equals(Errors.zeroErrors)) {
          throw new RuntimeException(
            InterfaceReader.InterfaceReaderError.treeReport(errors).toString
          )
        }
        logger.trace(s"Daml-LF Archive decoded, packageId '${interface.packageId}'")
        (interface, interface.packageId -> pkgPrefix)
      }
    }

    val interfaces = interfacesAndPrefixes.map(_._1)
    val prefixes = interfacesAndPrefixes.collect { case (_, (key, Some(value))) =>
      (key, value)
    }.toMap
    (interfaces, prefixes)
  }

  private[CodeGenRunner] def generateFile(
      outputFile: Path,
      dataTypes: ImmArray[DefDataType.FWT],
      templates: ImmArray[DefTemplate.FWT],
  ): Unit = {
    logger.warn(
      s"Started writing file '$outputFile' with data types ${dataTypes.toString} and templates ${templates.toString}"
    )
    val _ = Files.createDirectories(outputFile.getParent)
    if (!Files.exists(outputFile)) {
      val _ = Files.createFile(outputFile)
    }
    val os = Files.newOutputStream(
      outputFile,
      StandardOpenOption.WRITE,
      StandardOpenOption.TRUNCATE_EXISTING,
    )
    os.close()
    logger.warn(s"Finish writing file '$outputFile'")
  }

  /** Given the package prefixes specified per DAR and the module-prefixes specified in
    * daml.yaml, produce the combined prefixes per package id.
    */
  private[codegen] def resolvePackagePrefixes(
      pkgPrefixes: Map[PackageId, String],
      modulePrefixes: Map[PackageReference, String],
      interfaces: Seq[Interface],
  ): Map[PackageId, String] = {
    val metadata: Map[PackageReference.NameVersion, PackageId] = interfaces.view
      .flatMap(iface =>
        iface.metadata.iterator.map(metadata =>
          PackageReference.NameVersion(metadata.name, metadata.version) -> iface.packageId
        )
      )
      .toMap
    def resolveRef(ref: PackageReference): PackageId = ref match {
      case nameVersion: PackageReference.NameVersion =>
        metadata.getOrElse(
          nameVersion,
          throw new IllegalArgumentException(
            s"""No package $nameVersion found, available packages: ${metadata.keys.mkString(
              ", "
            )}"""
          ),
        )
    }
    val resolvedModulePrefixes: Map[PackageId, String] = modulePrefixes.map { case (k, v) =>
      resolveRef(k) -> v.toLowerCase
    }
    (pkgPrefixes.keySet union resolvedModulePrefixes.keySet).view.map { k =>
      val prefix = (pkgPrefixes.get(k), resolvedModulePrefixes.get(k)) match {
        case (None, None) =>
          throw new RuntimeException(
            "Internal error: key in pkgPrefixes and resolvedModulePrefixes could not be found in either of them"
          )
        case (Some(a), None) => a.stripSuffix(".")
        case (None, Some(b)) => b.stripSuffix(".")
        case (Some(a), Some(b)) => s"""${a.stripSuffix(".")}.${b.stripSuffix(".")}"""
      }
      k -> prefix
    }.toMap
  }

  /** Verify that no two module names collide when the given
    * prefixes are applied.
    */
  private[codegen] def detectModuleCollisions(
      pkgPrefixes: Map[PackageId, String],
      interfaces: Seq[Interface],
  ): Unit = {
    val allModules: Seq[(String, PackageId)] =
      for {
        interface <- interfaces
        modules = interface.typeDecls.keySet.map(_.module)
        module <- modules
        maybePrefix = pkgPrefixes.get(interface.packageId)
        prefixedName = maybePrefix.fold(module.toString)(prefix => s"$prefix.$module")
      } yield prefixedName -> interface.packageId
    allModules.groupBy(_._1).foreach { case (m, grouped) =>
      if (grouped.length > 1) {
        val pkgIds = grouped.view.map(_._2).mkString(", ")
        throw new IllegalArgumentException(
          s"""Duplicate module $m found in multiple packages $pkgIds. To resolve the conflict rename the modules in one of the packages via `module-prefixes`."""
        )
      }
    }
  }

  private[CodeGenRunner] def generateCode(
      interfaces: Seq[Interface],
      conf: Conf,
      pkgPrefixes: Map[PackageId, String],
  )(implicit ec: ExecutionContext): Unit = {
    logger.info(
      s"Start processing packageIds '${interfaces.map(_.packageId).mkString(", ")}' in directory '${conf.outputDirectory}'"
    )

    val prefixes = resolvePackagePrefixes(pkgPrefixes, conf.modulePrefixes, interfaces)
    detectModuleCollisions(prefixes, interfaces)

    // TODO (mp): pre-processing and escaping
    val preprocessingFuture: Future[InterfaceTrees] =
      backend.preprocess(interfaces, conf, prefixes)

    val future: Future[Unit] = {
      for {
        preprocessedInterfaceTrees <- preprocessingFuture
        _ <- Future.traverse(preprocessedInterfaceTrees.interfaceTrees)(
          processInterfaceTree(_, conf, prefixes)
        )
      } yield ()
    }

    // TODO (mp): make the timeout configurable
    val _ = Await.result(future, Duration.create(10L, TimeUnit.MINUTES))
    logger.info(s"Finish processing packageIds ''${interfaces.map(_.packageId).mkString(", ")}''")
  }

  // TODO (#584): Make Java Codegen Backend configurable
  private[codegen] val backend: Backend = JavaBackend

  private[CodeGenRunner] def processInterfaceTree(
      interfaceTree: InterfaceTree,
      conf: Conf,
      packagePrefixes: Map[PackageId, String],
  )(implicit ec: ExecutionContext): Future[Unit] = {
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
        s"Output directory '$outputPath' exists but it is not a directory"
      )
    }
  }
}
