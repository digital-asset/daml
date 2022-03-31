// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.nio.file.Path
import java.util.concurrent.TimeUnit

import com.daml.lf.archive.DarParser
import com.daml.lf.codegen.backend.Backend
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.codegen.conf.{PackageReference, Conf}

import scala.collection.immutable.Map
import com.daml.lf.iface.{EnvironmentInterface, Interface}
import com.daml.lf.iface.reader.{InterfaceReader, Errors}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Await, ExecutionContext}

final class CodeGen(
    backend: Backend,
    interfaces: Seq[Interface],
    prefixes: Map[PackageId, String],
    decoderPkgAndClass: Option[(String, String)],
    outputDirectory: Path,
) extends StrictLogging {

  private def processInterfaceTree(
      interfaceTree: InterfaceTree
  )(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info(s"Start processing packageId '${interfaceTree.interface.packageId}'")
    for {
      _ <- interfaceTree.process(backend.process(_, prefixes, outputDirectory))
    } yield {
      logger.info(s"Stop processing packageId '${interfaceTree.interface.packageId}'")
    }
  }

  def runWith(executionContext: ExecutionContext): Unit = {

    implicit val ec: ExecutionContext = executionContext

    val packageIds = interfaces.map(_.packageId).mkString(", ")
    logger.info(s"Start processing packageIds '$packageIds' in directory '$outputDirectory'")

    val future: Future[Unit] = {
      for {
        interfaceTrees <- backend.preprocess(
          interfaces,
          outputDirectory,
          decoderPkgAndClass,
          prefixes,
        )
        _ <- Future.traverse(interfaceTrees)(processInterfaceTree(_))
      } yield ()
    }
    // TODO: make the timeout configurable
    val _ = Await.result(future, Duration.create(10L, TimeUnit.MINUTES))
    logger.info(s"Finish processing packageIds '$packageIds'")
  }

}

object CodeGen extends StrictLogging {

  private def decodeDarAt(path: Path): Seq[Interface] =
    for (archive <- DarParser.assertReadArchiveFromFile(path.toFile).all) yield {
      val (errors, interface) = Interface.read(archive)
      if (!errors.equals(Errors.zeroErrors)) {
        throw new RuntimeException(
          InterfaceReader.InterfaceReaderError.treeReport(errors).toString
        )
      }
      logger.trace(s"Daml-LF Archive decoded, packageId '${interface.packageId}'")
      interface
    }

  private def resolveChoices(environmentInterface: EnvironmentInterface): Interface => Interface =
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

  def configure(backend: Backend, conf: Conf): CodeGen = {
    val (interfaces, pkgPrefixes) = collectDamlLfInterfaces(conf.darFiles)
    val prefixes = resolvePackagePrefixes(pkgPrefixes, conf.modulePrefixes, interfaces)
    detectModuleCollisions(prefixes, interfaces)
    new CodeGen(
      backend,
      interfaces,
      prefixes,
      conf.decoderPkgAndClass,
      conf.outputDirectory,
    )
  }

}
