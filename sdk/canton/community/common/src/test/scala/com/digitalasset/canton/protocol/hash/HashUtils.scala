// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.daml.crypto.MessageDigestPrototype
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.HashTracer.StringHashTracer
import com.digitalasset.daml.lf.data.Ref.IdString
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node, TransactionVersion}
import com.digitalasset.daml.lf.value.{Value, Value as V}
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util.UUID
import scala.collection.immutable.{SortedMap, SortedSet}

trait HashUtilsTest { this: Matchers =>
  val packageId0: IdString.PackageId = Ref.PackageId.assertFromString("package")
  val packageName0: IdString.PackageName = Ref.PackageName.assertFromString("package-name-0")

  implicit val contractIdOrdering: Ordering[Value.ContractId] = Ordering.by(_.coid)
  val transactionUUID = UUID.fromString("4c6471d3-4e09-49dd-addf-6cd90e19c583")
  val cid1 = Value.ContractId.assertFromString(
    "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
  )
  val cid2 = Value.ContractId.assertFromString(
    "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
  )
  val alice = Ref.Party.assertFromString("alice")
  val bob = Ref.Party.assertFromString("bob")
  val node1 = FatContractInstance.fromCreateNode(
    dummyCreateNode(cid1, Set(alice), Set(alice)),
    Time.Timestamp.Epoch.add(Duration.ofDays(10)),
    Bytes.assertFromString("0010"),
  )
  val node2 = FatContractInstance.fromCreateNode(
    dummyCreateNode(cid2, Set(bob), Set(bob)),
    Time.Timestamp.Epoch.add(Duration.ofDays(20)),
    Bytes.assertFromString("0050"),
  )
  val metadata = TransactionMetadataHashBuilder.MetadataV1(
    actAs = SortedSet(alice, bob),
    commandId = Ref.CommandId.assertFromString("command-id"),
    transactionUUID = transactionUUID,
    mediatorGroup = 0,
    synchronizerId = "synchronizerId",
    ledgerEffectiveTime = Some(Time.Timestamp.Epoch),
    submissionTime = Time.Timestamp.Epoch,
    disclosedContracts = SortedMap(
      cid1 -> node1,
      cid2 -> node2,
    ),
  )

  def assertStringTracer(stringHashTracer: StringHashTracer, hash: Hash) = {
    val messageDigest = MessageDigestPrototype.Sha256.newDigest
    messageDigest.update(stringHashTracer.asByteArray)
    Hash.tryFromByteStringRaw(ByteString.copyFrom(messageDigest.digest()), Sha256) shouldBe hash
  }

  def cid(s: String): V.ContractId = V.ContractId.V1(LfHash.hashPrivateKey(s))

  def dummyCreateNode(
      createCid: V.ContractId,
      signatories: Set[Ref.Party] = Set.empty,
      stakeholders: Set[Ref.Party] = Set.empty,
  ): Node.Create =
    Node.Create(
      coid = createCid,
      packageName = Ref.PackageName.assertFromString("PkgName"),
      packageVersion = None,
      templateId = Ref.Identifier.assertFromString("-dummyPkg-:DummyModule:dummyName"),
      arg = V.ValueContractId(cid("#dummyCid")),
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = None,
      version = TransactionVersion.minVersion,
    )

  def defRef(module: String, name: String): Ref.Identifier =
    Ref.Identifier(
      packageId0,
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(module),
        Ref.DottedName.assertFromString(name),
      ),
    )
}
