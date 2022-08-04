// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.packagemeta

import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.daml_lf_dev.DamlLf
import com.daml.platform.store.packagemeta.PackageMetadataView._
import scalaz._

trait PackageMetadataView {
  def update(metadataDefinitions: MetadataDefinitions): Unit

  def read(): PackageMetadata
}

object PackageMetadataView {
  def create: PackageMetadataView = new PackageMetaDataViewImpl

  trait PackageMetadata {
    def interfaceImplementedBy(interface: Ref.Identifier): Set[Ref.Identifier]
    def interfaceExists(interface: Ref.Identifier): Boolean
    def templateExists(template: Ref.Identifier): Boolean
  }

  final case class MetadataDefinitions private[packagemeta] (
      templates: Set[Ref.Identifier] = Set.empty,
      interfaces: Set[Ref.Identifier] = Set.empty,
      interfacesImplementedBy: Map[Ref.Identifier, Set[Ref.Identifier]] = Map.empty,
  )

  object MetadataDefinitions {
    implicit val monoid = new Monoid[MetadataDefinitions] {
      override def zero: MetadataDefinitions = MetadataDefinitions()
      override def append(
          f1: MetadataDefinitions,
          f2: => MetadataDefinitions,
      ): MetadataDefinitions =
        MetadataDefinitions(
          templates = f1.templates ++ f2.templates,
          interfaces = f1.interfaces ++ f2.interfaces,
          interfacesImplementedBy =
            f2.interfacesImplementedBy.foldLeft(f1.interfacesImplementedBy) {
              case (acc, (interface, templates)) =>
                acc + (interface -> (acc.getOrElse(interface, Set.empty) ++ templates))
            },
        )
    }
    implicit def equal: Equal[MetadataDefinitions] =
      (a1: MetadataDefinitions, a2: MetadataDefinitions) => a1 == a2

    def from(archive: DamlLf.Archive): MetadataDefinitions = {
      val packageInfo = Decode.assertDecodeInfoPackage(archive)
      MetadataDefinitions(
        templates = packageInfo.definedTemplates,
        interfaces = packageInfo.definedInterfaces,
        interfacesImplementedBy = packageInfo.interfaceInstances,
      )
    }

    private[packagemeta] implicit class MetadataDefinitionsToPackageMetadata(
        metadataDefinitions: MetadataDefinitions
    ) extends PackageMetadata {
      override def interfaceImplementedBy(interface: Ref.Identifier): Set[Ref.Identifier] =
        metadataDefinitions.interfacesImplementedBy.getOrElse(interface, Set.empty)

      override def interfaceExists(interface: Ref.Identifier): Boolean =
        metadataDefinitions.interfaces(interface)

      override def templateExists(template: Ref.Identifier): Boolean =
        metadataDefinitions.templates(template)
    }
  }
}

private[packagemeta] class PackageMetaDataViewImpl extends PackageMetadataView {
  @volatile private var metadataDefinitions = MetadataDefinitions.monoid.zero

  private def set(metadataDefinitions: MetadataDefinitions): Unit =
    this.metadataDefinitions = metadataDefinitions

  override def update(newMetadataDefinitions: MetadataDefinitions): Unit = {
    synchronized {
      set(MetadataDefinitions.monoid.append(metadataDefinitions, newMetadataDefinitions))
    }
  }

  override def read(): PackageMetadata = metadataDefinitions
}
