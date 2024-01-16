// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.packagemeta

import cats.implicits.catsSyntaxSemigroup

import java.util.concurrent.atomic.AtomicReference

import PackageMetadata.Implicits.packageMetadataSemigroup

trait PackageMetadataView {
  def update(packageMetadata: PackageMetadata): Unit

  def current(): PackageMetadata
}

object PackageMetadataView {
  def create: PackageMetadataView = new PackageMetaDataViewImpl
}

private[packagemeta] class PackageMetaDataViewImpl extends PackageMetadataView {
  private val packageMetadataRef = new AtomicReference(PackageMetadata())

  override def update(other: PackageMetadata): Unit =
    packageMetadataRef.updateAndGet(_ |+| other).discard

  override def current(): PackageMetadata = packageMetadataRef.get()
}
