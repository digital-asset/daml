// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory}

import com.daml.lf.archive.DarParser
import com.daml.lf.codegen.backend.java.inner.{ClassForType, DecoderClass, fullyQualifiedName}
import com.daml.lf.codegen.conf.{Conf, PackageReference}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.iface.reader.{InterfaceReader, Errors}
import com.daml.lf.iface.{EnvironmentInterface, InterfaceType, Interface}
import com.squareup.javapoet.{JavaFile, ClassName}
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.{LoggerFactory, Logger, MDC}

import scala.collection.immutable.Map
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutorService, Await, ExecutionContext, Future}

private final class CodeGenRunner(
    interfaces: Seq[Interface],
    outputDirectory: Path,
    decoderPkgAndClass: Option[(String, String)],
    packagePrefixes: Map[PackageId, String],
) extends StrictLogging {

  def runWith(executionContext: ExecutionContext): Future[Unit] = {
    implicit val ec: ExecutionContext = executionContext
    val packageIds = interfaces.map(_.packageId).mkString(", ")
    logger.info(s"Start processing packageIds '$packageIds'")
    for {
      interfaceTrees <- Future.successful(interfaces.map(InterfaceTree.fromInterface))
      _ <- generateDecoder(interfaceTrees)
      _ <- Future.traverse(interfaceTrees)(processInterfaceTree(_))
    } yield logger.info(s"Finished processing packageIds '$packageIds'")
  }

  private def generateDecoder(
      interfaceTrees: Seq[InterfaceTree]
  )(implicit ec: ExecutionContext): Future[Unit] = {
    decoderPkgAndClass.fold(Future.unit) { case (decoderPackage, decoderClassName) =>
      val templateNames = extractTemplateNames(interfaceTrees)
      val decoderClass = DecoderClass.generateCode(decoderClassName, templateNames)
      val decoderFile = JavaFile.builder(decoderPackage, decoderClass).build()
      Future(decoderFile.writeTo(outputDirectory))
    }
  }

  private def extractTemplateNames(
      interfaceTrees: Seq[InterfaceTree]
  ): Seq[ClassName] = {
    interfaceTrees.flatMap(_.bfs(Vector[ClassName]()) {
      case (res, module: ModuleWithContext) =>
        val templateNames = module.typesLineages.collect {
          case t if t.`type`.typ.exists(CodeGenRunner.isTemplate) =>
            ClassName.bestGuess(fullyQualifiedName(t.identifier, packagePrefixes))
        }
        res ++ templateNames
      case (res, _) => res
    })
  }

  private def processInterfaceTree(
      interfaceTree: InterfaceTree
  )(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info(s"Start processing packageId '${interfaceTree.interface.packageId}'")
    for (_ <- interfaceTree.process(process))
      yield logger.info(s"Finished processing packageId '${interfaceTree.interface.packageId}'")
  }

  private def process(
      nodeWithContext: NodeWithContext
  )(implicit ec: ExecutionContext): Future[Unit] =
    nodeWithContext match {
      case moduleWithContext: ModuleWithContext =>
        // this is a Daml module that contains type declarations => the codegen will create one file
        val moduleName = moduleWithContext.lineage.map(_._1).toSeq.mkString(".")
        Future {
          logger.info(s"Generating code for module $moduleName")
          for (javaFile <- createTypeDefinitionClasses(moduleWithContext)) {
            val javaFileFullName = s"${javaFile.packageName}.${javaFile.typeSpec.name}"
            logger.info(s"Writing $javaFileFullName to directory $outputDirectory")
            javaFile.writeTo(outputDirectory)
          }
        }
      case _ =>
        Future.unit
    }

  private def createTypeDefinitionClasses(
      moduleWithContext: ModuleWithContext
  ): Iterable[JavaFile] = {
    MDC.put("packageId", moduleWithContext.packageId)
    MDC.put("packageIdShort", moduleWithContext.packageId.take(7))
    MDC.put("moduleName", moduleWithContext.name)
    val typeSpecs = moduleWithContext.typesLineages.flatMap(ClassForType(_, packagePrefixes))
    MDC.remove("packageId")
    MDC.remove("packageIdShort")
    MDC.remove("moduleName")
    typeSpecs
  }

}

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

    val (interfaces, pkgPrefixes) = collectDamlLfInterfaces(conf.darFiles)
    val prefixes = resolvePackagePrefixes(pkgPrefixes, conf.modulePrefixes, interfaces)
    detectModuleCollisions(prefixes, interfaces)

    val codegen =
      new CodeGenRunner(interfaces, conf.outputDirectory, conf.decoderPkgAndClass, prefixes)
    val executionContext: ExecutionContextExecutorService = createExecutionContext()
    val result = codegen.runWith(executionContext)
    Await.result(result, 10.minutes)
    executionContext.shutdownNow()

    ()
  }

  private def createExecutionContext(): ExecutionContextExecutorService =
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

  private[codegen] def decodeDarAt(path: Path): Seq[Interface] =
    for (archive <- DarParser.assertReadArchiveFromFile(path.toFile).all) yield {
      val (errors, interface) = Interface.read(archive)
      if (!errors.equals(Errors.zeroErrors)) {
        val message = InterfaceReader.InterfaceReaderError.treeReport(errors).toString
        throw new RuntimeException(message)
      }
      logger.trace(s"Daml-LF Archive decoded, packageId '${interface.packageId}'")
      interface
    }

  private[codegen] def resolveChoices(
      environmentInterface: EnvironmentInterface
  ): Interface => Interface =
    _.resolveChoicesAndFailOnUnresolvableChoices(environmentInterface.astInterfaces)

  private[codegen] def collectDamlLfInterfaces(
      darFiles: Iterable[(Path, Option[String])]
  ): (Seq[Interface], Map[PackageId, String]) = {
    val interfacesAndPrefixes =
      for {
        (path, packagePrefix) <- darFiles.view
        interface <- decodeDarAt(path)
      } yield (interface, packagePrefix)
    val (interfaces, prefixes) =
      interfacesAndPrefixes.foldLeft((Vector.empty[Interface], Map.empty[PackageId, String])) {
        case ((interfaces, prefixes), (interface, prefix)) =>
          val updatedInterfaces = interfaces :+ interface
          val updatedPrefixes = prefix.fold(prefixes)(prefixes.updated(interface.packageId, _))
          (updatedInterfaces, updatedPrefixes)
      }

    val environmentInterface = EnvironmentInterface.fromReaderInterfaces(interfaces)
    val resolvedInterfaces = interfaces.map(resolveChoices(environmentInterface))
    (resolvedInterfaces, prefixes)
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
        val packages = metadata.keys.mkString(", ")
        metadata.getOrElse(
          nameVersion,
          throw new IllegalArgumentException(
            s"No package $nameVersion found, available packages: $packages"
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

  private def isTemplate(lfInterfaceType: InterfaceType): Boolean =
    lfInterfaceType match {
      case _: InterfaceType.Template => true
      case _ => false
    }

  private def assertInputFileExists(filePath: Path): Unit = {
    logger.trace(s"Checking that the file '$filePath' exists")
    if (Files.notExists(filePath)) {
      throw new IllegalArgumentException(s"Input file '$filePath' doesn't exist")
    }
  }

  private def assertInputFileIsReadable(filePath: Path): Unit = {
    logger.trace(s"Checking that the file '$filePath' is readable")
    if (!Files.isReadable(filePath)) {
      throw new IllegalArgumentException(s"Input file '$filePath' is not readable")
    }
  }

  private def checkAndCreateOutputDir(outputPath: Path): Unit = {
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
