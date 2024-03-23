// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction
package test

import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.IdString
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

  def newParty: IdString.Party = Ref.Party.assertFromString("party" + nextSuffix)

  def newPackageId: IdString.PackageId = Ref.PackageId.assertFromString("pkgId" + nextSuffix)

  def newModName: Ref.DottedName = Ref.DottedName.assertFromString("Mod" + nextSuffix)

  def newChoiceName: IdString.Name = Ref.Name.assertFromString("Choice" + nextSuffix)

  def newIdentifierName: Ref.DottedName = Ref.DottedName.assertFromString("T" + nextSuffix)

  def newIdentifier: Ref.Identifier =
    Ref.Identifier(newPackageId, Ref.QualifiedName(newModName, newIdentifierName))

}
