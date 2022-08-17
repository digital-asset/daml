// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.packagemeta

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.Decode
import com.daml.platform.packagemeta.PackageMetadata

trait PackageMetadataView {
  def update(packageMetadata: PackageMetadata): Unit

  def current(): PackageMetadata
}

object PackageMetadataView {
  def create: PackageMetadataView = new PackageMetaDataViewImpl

  def fromArchive(archive: DamlLf.Archive): PackageMetadata = {
    val packageInfo = Decode.assertDecodeInfoPackage(archive)
    PackageMetadata(
      templates = packageInfo.definedTemplates,
      interfaces = packageInfo.definedInterfaces,
      interfacesImplementedBy = packageInfo.interfaceInstances,
    )
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
