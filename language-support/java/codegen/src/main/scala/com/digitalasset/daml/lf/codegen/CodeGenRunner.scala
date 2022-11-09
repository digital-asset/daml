// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory}
import com.daml.lf.archive.DarParser
import com.daml.lf.codegen.backend.java.inner.{ClassForType, DecoderClass, fullyQualifiedName}
import com.daml.lf.codegen.conf.{Conf, PackageReference}
import com.daml.lf.codegen.dependencygraph.DependencyGraph
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.typesig.reader.{Errors, SignatureReader}
import com.daml.lf.typesig.{EnvironmentSignature, PackageSignature}
import PackageSignature.TypeDecl
import com.daml.lf.language.Reference
import com.squareup.javapoet.{ClassName, JavaFile}
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.{Logger, LoggerFactory, MDC}

import scala.collection.immutable.Map
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

private final class CodeGenRunner(
    scopeByPrefix: Map[Option[String], CodeGenRunner.Scope],
    outputDirectory: Path,
    decoderPackageAndClass: Option[(String, String)],
) extends StrictLogging {

  def runWith(executionContext: ExecutionContext): Future[Unit] = {
    implicit val ec: ExecutionContext = executionContext
    Future
      .traverse(scopeByPrefix.toSeq) { case (maybePrefix, scope) =>
        val packageIds = scope.signatures.map(_.packageId).mkString(", ")
        val prefix = maybePrefix.fold("")(p => s" with prefix '$p'")
        logger.info(
          s"Start processing packageIds '$packageIds'$prefix"
        )
        for {
          _ <- generateDecoder()
          interfaceTrees = scope.signatures.map(InterfaceTree.fromInterface)
          _ <- Future.traverse(interfaceTrees)(processInterfaceTree(scope, _))
        } yield logger.info(s"Finished processing packageIds '$packageIds'$prefix")
      }
      .map(_ => ())
  }

  private def generateDecoder()(implicit ec: ExecutionContext): Future[Unit] =
    decoderPackageAndClass.fold(Future.unit) { case (decoderPackage, decoderClassName) =>
      val decoderClass = DecoderClass.generateCode(
        decoderClassName,
        scopeByPrefix.view.values.flatMap(s => s.templateClassNames),
      )
      val decoderFile = JavaFile.builder(decoderPackage, decoderClass).build()
      Future(decoderFile.writeTo(outputDirectory))
    }

  private def processInterfaceTree(
      scope: CodeGenRunner.Scope,
      interfaceTree: InterfaceTree,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info(s"Start processing packageId '${interfaceTree.interface.packageId}'")
    for (_ <- interfaceTree.process(process(scope, _)))
      yield logger.info(s"Finished processing packageId '${interfaceTree.interface.packageId}'")
  }

  private def process(
      scope: CodeGenRunner.Scope,
      nodeWithContext: NodeWithContext,
  )(implicit ec: ExecutionContext): Future[Unit] =
    nodeWithContext match {
      case moduleWithContext: ModuleWithContext =>
        // this is a Daml module that contains type declarations => the codegen will create one file
        val moduleName = moduleWithContext.lineage.map(_._1).toSeq.mkString(".")
        Future {
          logger.info(s"Generating code for module $moduleName")
          for (javaFile <- createTypeDefinitionClasses(scope, moduleWithContext)) {
            val javaFileFullName = s"${javaFile.packageName}.${javaFile.typeSpec.name}"
            logger.info(s"Writing $javaFileFullName to directory $outputDirectory")
            javaFile.writeTo(outputDirectory)
          }
        }
      case _ =>
        Future.unit
    }

  private def createTypeDefinitionClasses(
      scope: CodeGenRunner.Scope,
      module: ModuleWithContext,
  ): Iterable[JavaFile] = {
    MDC.put("packageId", module.packageId)
    MDC.put("packageIdShort", module.packageId.take(7))
    MDC.put("moduleName", module.name)
    val javaFiles =
      for {
        typeWithContext <- module.typesLineages
        javaFile <- ClassForType(typeWithContext, scope.packagePrefixes, scope.toBeGenerated)
      } yield javaFile
    MDC.remove("packageId")
    MDC.remove("packageIdShort")
    MDC.remove("moduleName")
    javaFiles
  }

}

object CodeGenRunner extends StrictLogging {

  private[codegen] final class Scope(
      val signatures: Seq[PackageSignature],
      val packagePrefixes: Map[PackageId, String],
      serializableTypes: Vector[(Identifier, TypeDecl)],
  ) {

    val toBeGenerated: Set[Identifier] = serializableTypes.view.map(_._1).toSet

    val templateClassNames: Vector[ClassName] = serializableTypes.collect {
      case id -> (_: TypeDecl.Template) =>
        ClassName.bestGuess(fullyQualifiedName(id, packagePrefixes))
    }

  }

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

    val scopeByPrefix = configureCodeGenScopeByPackagePrefix(conf.darFiles, conf.modulePrefixes)

    val codegen = new CodeGenRunner(scopeByPrefix, conf.outputDirectory, conf.decoderPkgAndClass)
    val executionContext: ExecutionContextExecutorService = createExecutionContext()
    val result = codegen.runWith(executionContext)
    Await.result(result, 10.minutes)
    executionContext.shutdownNow()

    ()
  }

  private[codegen] def configureCodeGenScopeByPackagePrefix(
      darFiles: Iterable[(Path, Option[String])],
      modulePrefixes: Map[PackageReference, String],
  ): Map[Option[String], CodeGenRunner.Scope] = {
    val pathsByPackagePrefix = darFiles.groupMap(_._2)(_._1)
    pathsByPackagePrefix.map { case (maybePrefix, paths) =>
      maybePrefix -> configureCodeGenScope(maybePrefix, paths, modulePrefixes)
    }
  }

  private[codegen] def configureCodeGenScope(
      packagePrefix: Option[String],
      darFiles: Iterable[Path],
      modulePrefixes: Map[PackageReference, String],
  ): CodeGenRunner.Scope = {
    val signatureMap = (for {
      path <- darFiles
      signature <- decodeDarAt(path)
    } yield signature.packageId -> signature).toMap

    val signatures = signatureMap.values.toSeq
    val environmentInterface = EnvironmentSignature.fromPackageSignatures(signatures)

    val transitiveClosure = DependencyGraph.transitiveClosure(
      environmentInterface.typeDecls,
      environmentInterface.interfaces,
    )
    for (error <- transitiveClosure.errors) {
      logger.error(error.msg)
    }
    val generatedModuleIds: Set[Reference.Module] = (
      transitiveClosure.serializableTypes.map(_._1) ++
        transitiveClosure.interfaces.map(_._1)
    ).toSet.map { id: Identifier =>
      Reference.Module(id.packageId, id.qualifiedName.module)
    }

    val resolvedSignatures = resolveRetroInterfaces(signatures)

    val resolvedPrefixes =
      resolvePackagePrefixes(
        packagePrefix,
        modulePrefixes,
        resolvedSignatures,
        generatedModuleIds,
      )

    new CodeGenRunner.Scope(
      resolvedSignatures,
      resolvedPrefixes,
      transitiveClosure.serializableTypes,
    )
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

  private[codegen] def decodeDarAt(path: Path): Seq[PackageSignature] =
    for (archive <- DarParser.assertReadArchiveFromFile(path.toFile).all) yield {
      val (errors, interface) = PackageSignature.read(archive)
      if (!errors.equals(Errors.zeroErrors)) {
        val message = SignatureReader.Error.treeReport(errors).toString
        throw new RuntimeException(message)
      }
      logger.trace(s"Daml-LF Archive decoded, packageId '${interface.packageId}'")
      interface
    }

  private[this] def resolveRetroInterfaces(
      signatures: Seq[PackageSignature]
  ): Seq[PackageSignature] =
    PackageSignature.resolveRetroImplements((), signatures)((_, _) => None)._2

  /** Given the package prefixes specified per DAR and the module-prefixes specified in
    * daml.yaml, produce the combined prefixes per package id.
    */
  private[codegen] def resolvePackagePrefixes(
      packagePrefix: Option[String],
      modulePrefixes: Map[PackageReference, String],
      signatures: Seq[PackageSignature],
      generatedModules: Set[Reference.Module],
  ): Map[PackageId, String] = {
    val metadata: Map[PackageReference.NameVersion, PackageId] = signatures.view
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
    val resolvedPackagePrefixes =
      (packagePrefix.fold(Set.empty[PackageId])(_ =>
        signatures.map(_.packageId).toSet
      ) union resolvedModulePrefixes.keySet).view.map { k =>
        val prefix = (packagePrefix, resolvedModulePrefixes.get(k)) match {
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
    detectModuleCollisions(resolvedPackagePrefixes, signatures, generatedModules)
    resolvedPackagePrefixes
  }

  /** Verify that no two module names collide when the given
    * prefixes are applied.
    */
  private[codegen] def detectModuleCollisions(
      pkgPrefixes: Map[PackageId, String],
      interfaces: Seq[PackageSignature],
      generatedModules: Set[Reference.Module],
  ): Unit = {
    val allModules: Seq[(String, PackageId)] =
      for {
        interface <- interfaces
        modules = interface.typeDecls.keySet.map(_.module)
        module <- modules
        maybePrefix = pkgPrefixes.get(interface.packageId)
        prefixedName = maybePrefix.fold(module.toString)(prefix => s"$prefix.$module")
        if generatedModules.contains(Reference.Module(interface.packageId, module))
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
