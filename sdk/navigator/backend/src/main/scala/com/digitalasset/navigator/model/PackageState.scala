// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model

import com.daml.lf.data.Ref
import com.daml.lf.typesig

import scala.collection.immutable.Map

final case class PackageState(packages: PackageState.PackageStore) {
  import PackageState.PackageStore
  def append(diff: PackageStore): PackageState = {
    val newPackages = resolveChoicesIn(
      appendAndResolveRetroactiveInterfaces(diff)
    )
    copy(packages = newPackages)
  }

  private[this] def appendAndResolveRetroactiveInterfaces(diff: PackageStore): PackageStore = {
    def lookupIf(packageStore: PackageStore, pkId: Ref.PackageId) =
      packageStore
        .get(pkId)
        .map((_, { newSig: typesig.PackageSignature => packageStore.updated(pkId, newSig) }))

    val (packageStore2, diffElems) =
      typesig.PackageSignature.resolveRetroImplements(packages, diff.values.toSeq)(lookupIf)
    packageStore2 ++ diffElems.view.map(p => (p.packageId, p))
  }

  private[this] def resolveChoicesIn(diff: PackageStore): PackageStore = {
    def lookupIf(pkgId: Ref.PackageId) = (packages get pkgId) orElse (diff get pkgId)
    val findIface = typesig.PackageSignature.findInterface(Function unlift lookupIf)
    diff.transform((_, iface) => iface resolveChoicesAndFailOnUnresolvableChoices findIface)
  }
}

object PackageState {
  type PackageStore = Map[String, typesig.PackageSignature]
}
