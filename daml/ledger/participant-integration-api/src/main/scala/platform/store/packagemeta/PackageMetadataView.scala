// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.packagemeta

import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.daml_lf_dev.DamlLf
import com.daml.platform.store.packagemeta.PackageMetadataView._

trait PackageMetadataView {
  def update(packageMetadata: PackageMetadata): Unit

  def current(): PackageMetadata
}

object PackageMetadataView {
  def create: PackageMetadataView = new PackageMetaDataViewImpl

  final case class PackageMetadata private[packagemeta] (
      templates: Set[Ref.Identifier] = Set.empty,
      interfaces: Set[Ref.Identifier] = Set.empty,
      interfacesImplementedBy: Map[Ref.Identifier, Set[Ref.Identifier]] = Map.empty,
  ) {
    def append(
        updated: PackageMetadata
    ): PackageMetadata =
      PackageMetadata(
        templates = templates ++ updated.templates,
        interfaces = interfaces ++ updated.interfaces,
        interfacesImplementedBy =
          updated.interfacesImplementedBy.foldLeft(interfacesImplementedBy) {
            case (acc, (interface, templates)) =>
              acc + (interface -> (acc.getOrElse(interface, Set.empty) ++ templates))
          },
      )
  }

  object PackageMetadata {
    def from(archive: DamlLf.Archive): PackageMetadata = {
      val packageInfo = Decode.assertDecodeInfoPackage(archive)
      PackageMetadata(
        templates = packageInfo.definedTemplates,
        interfaces = packageInfo.definedInterfaces,
        interfacesImplementedBy = packageInfo.interfaceInstances,
      )
    }
  }
}

private[packagemeta] class PackageMetaDataViewImpl extends PackageMetadataView {
  @volatile private var packageMetadata = PackageMetadata()

  override def update(packageMetadata: PackageMetadata): Unit =
    synchronized {
      this.packageMetadata = this.packageMetadata.append(packageMetadata)
    }

  override def current(): PackageMetadata = packageMetadata
}
