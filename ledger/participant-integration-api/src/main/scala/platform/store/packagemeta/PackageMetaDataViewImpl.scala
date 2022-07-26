// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.packagemeta

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.DottedName

trait PackageMetadata {
  // TODO DPP-1068: [implementation detail] refactor this to non-option
  def interfaceImplementedBy(interface: Ref.Identifier): Option[Set[Ref.Identifier]]
  def interfaceExists(interface: Ref.Identifier): Boolean
  def templateExists(template: Ref.Identifier): Boolean
}

trait PackageMetadataView {
  // TODO DPP-1068: is it okay with this just to move forward? otherwise we need a reset too.
  def update(archive: Archive): Unit

  /** Computation will be executed with consistent snapshot of underlying data, and result will
    * be memoized, until re-computation needed (underlying data changed)
    */
  def apply[T](computation: PackageMetadata => T): () => T
}

case class MetadataDefinitions(
    templates: Set[Ref.Identifier] = Set.empty,
    interfaces: Set[Ref.Identifier] = Set.empty,
    interfacesImplementedBy: Map[Ref.Identifier, Set[Ref.Identifier]] = Map.empty,
) {
  def ++(additionalDefinitions: MetadataDefinitions): MetadataDefinitions =
    MetadataDefinitions(
      templates = templates ++ additionalDefinitions.templates,
      interfaces = interfaces ++ additionalDefinitions.interfaces,
      interfacesImplementedBy =
        additionalDefinitions.interfacesImplementedBy.foldLeft(interfacesImplementedBy) {
          case (acc, (interface, templates)) =>
            acc + (interface -> (acc.getOrElse(interface, Set.empty) ++ templates))
        },
    )
}

object MetadataDefinitions {
  // TODO DPP-1068: [implementation detail] this needs to be implemented by LF utility instead
  private[packagemeta] def from(archive: Archive): MetadataDefinitions =
    Decode
      .decodeArchive(archive, onlySerializableDataDefs = true) // TODO DPP-1068 why true?
      .fold(
        error => {
          // Note: some unit tests use dummy packages, and we want to gracefully handle broken packages
          // coming from the ReadService.
          // TODO DPP-1068: Proper error logging
          println(s"Archive could not be decoded: $error")
          MetadataDefinitions() // TODO DPP-1068: Shall we throw an exception instead?
        },
        result => {
          val (packageId, ast) = result
          ast.modules.iterator
            .map { case (moduleName, module) =>
              def identifier(name: DottedName) =
                Ref.Identifier(packageId, Ref.QualifiedName(moduleName, name))
              MetadataDefinitions(
                templates = module.templates.keysIterator.map(identifier).toSet,
                interfaces = module.interfaces.keysIterator.map(identifier).toSet,
                interfacesImplementedBy = module.templates.iterator
                  .flatMap { case (templateId, templateAst) =>
                    templateAst.implements.valuesIterator
                      .map(_.interfaceId -> identifier(templateId))
                  }
                  .toVector
                  .groupMapReduce(_._1) { case (_, templateId) =>
                    Set(templateId)
                  }(_ ++ _),
              )
            }
            .foldLeft(MetadataDefinitions())(_ ++ _)
        },
      )

  private[packagemeta] implicit class MetadataDefinitionsToPackageMetadata(
      metadataDefinitions: MetadataDefinitions
  ) extends PackageMetadata {

    override def interfaceImplementedBy(interface: Ref.Identifier): Option[Set[Ref.Identifier]] =
      metadataDefinitions.interfacesImplementedBy.get(interface)

    override def interfaceExists(interface: Ref.Identifier): Boolean =
      metadataDefinitions.interfaces(interface)

    override def templateExists(template: Ref.Identifier): Boolean =
      metadataDefinitions.templates(template)
  }
}

object PackageMetadataView {
  def create: PackageMetadataView = new PackageMetaDataViewImpl
}

private[packagemeta] class PackageMetaDataViewImpl extends PackageMetadataView {
  @volatile private var metadataDefinitions: Versioned[MetadataDefinitions] =
    Versioned(MetadataDefinitions())

  override def update(archive: Archive): Unit = {
    val newDefinitions = MetadataDefinitions.from(archive)
    synchronized {
      metadataDefinitions = metadataDefinitions.next(
        metadataDefinitions.value ++ newDefinitions
      )
    }
  }

  override def apply[T](computation: PackageMetadata => T): () => T = {
    val semaphore = new Object
    var memoizedResult: T = null.asInstanceOf[T]
    var memoizedVersion: Long =
      -1 // so we always compute first, even if metadataDefinitions.version is 0
    () =>
      semaphore.synchronized {
        if (metadataDefinitions.version > memoizedVersion) {
          val newDefinitions = metadataDefinitions
          memoizedResult = computation(newDefinitions.value)
          memoizedVersion = newDefinitions.version
        }
        memoizedResult
      }
  }
}

private[packagemeta] case class Versioned[T](value: T, version: Long = 0L) {
  def next(newValue: T): Versioned[T] = Versioned(newValue, version + 1)
}
