// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction
package test

import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageName
import com.daml.lf.value.Value.ContractId

import java.util.concurrent.atomic.AtomicInteger

trait TestIdFactory {

  private val atomic: AtomicInteger = new AtomicInteger()

  private val hashGen: () => crypto.Hash = {
    val bytes = Array.ofDim[Byte](crypto.Hash.underlyingHashLength)
    scala.util.Random.nextBytes(bytes)
    crypto.Hash.secureRandom(crypto.Hash.assertFromByteArray(bytes))
  }

  def nextHash(): Hash = hashGen()

  def nextSuffix: String = atomic.getAndIncrement().toString

  def newV1Cid: ContractId.V1 = ContractId.V1(nextHash())

  def newCid: ContractId = newV1Cid

  def newParty: Ref.Party = Ref.Party.assertFromString("party" + nextSuffix)

  def newPackageId: Ref.PackageId = Ref.PackageId.assertFromString("pkgId" + nextSuffix)

  def newModName: Ref.DottedName = Ref.DottedName.assertFromString("Mod" + nextSuffix)

  def newChoiceName: Ref.Name = Ref.Name.assertFromString("Choice" + nextSuffix)

  def newIdentifierName: Ref.DottedName = Ref.DottedName.assertFromString("T" + nextSuffix)

  def newPackageName: Ref.PackageName =
    Ref.PackageName.assertFromString("package-name-" + nextSuffix)

  def newPackageNameVersion: (PackageName, Ref.PackageVersion) = {
    val i = atomic.getAndIncrement()
    val pkgVersion = Ref.PackageVersion.assertFromInts(
      List((i >> 24) % 8, (i >> 16) % 8, (i >> 8) % 8, (i >> 0) % 8)
    )
    newPackageName -> pkgVersion
  }

  def newIdentifier: Ref.Identifier =
    Ref.Identifier(newPackageId, Ref.QualifiedName(newModName, newIdentifierName))

}
