// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.wasm

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.language.PackageInterface
import com.digitalasset.daml.lf.transaction.TransactionVersion

abstract class SpeedyUtils(pkgInterface: PackageInterface) {

  @throws[IllegalArgumentException]
  private[wasm] def zipSameLength[X, Y](xs: ImmArray[X], ys: ImmArray[Y]): ImmArray[(X, Y)] = {
    val n1 = xs.length
    val n2 = ys.length
    if (n1 != n2) {
      throw new IllegalArgumentException(s"sameLengthZip, $n1 /= $n2")
    }
    xs.zip(ys)
  }

  private[wasm] def tmplId2PackageNameVersion(
      tmplId: Ref.TypeConName
  ): (Ref.PackageName, Option[Ref.PackageVersion]) = {
    pkgInterface.signatures(tmplId.packageId).pkgNameVersion
  }

  private[wasm] def tmplId2TxVersion(tmplId: Ref.TypeConName): TransactionVersion = {
    TransactionVersion.assignNodeVersion(
      pkgInterface.packageLanguageVersion(tmplId.packageId)
    )
  }
}
