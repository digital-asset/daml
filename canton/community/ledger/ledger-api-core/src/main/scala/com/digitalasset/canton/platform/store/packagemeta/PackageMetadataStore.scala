// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.packagemeta

import com.daml.lf.data.Ref.{PackageId, PackageName, PackageVersion}
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.PackageResolution

// TODO(#16362): Consider extracting in ledger-common/com.digitalasset.canton.ledger.localstore
trait PackageMetadataStore {
  def getSnapshot: PackageMetadataSnapshot
}

class InMemoryPackageMetadataStore(
    packageMetadataView: PackageMetadataView
) extends PackageMetadataStore {
  override def getSnapshot: PackageMetadataSnapshot =
    new PackageMetadataSnapshot(packageMetadataView.current())
}

class PackageMetadataSnapshot(packageMetadataSnapshot: PackageMetadata) {
  def getUpgradablePackagePreferenceMap: Map[PackageName, PackageId] =
    packageMetadataSnapshot.packageNameMap.view.mapValues { case PackageResolution(preference, _) =>
      preference.packageId
    }.toMap

  def getUpgradablePackageMap: Map[PackageId, (PackageName, PackageVersion)] =
    packageMetadataSnapshot.packageIdVersionMap
}
