// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.BlindingSpec._
import com.daml.lf.ledger.BlindingTransaction
import com.daml.lf.transaction.test.TestNodeBuilder.CreateKey
import com.daml.lf.transaction.test.TreeTransactionBuilder.NodeOps
import com.daml.lf.transaction.test._
import com.daml.lf.transaction.{BlindingInfo, Node, VersionedTransaction}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ValueRecord, ValueTrue}
import com.digitalasset.daml.lf.transaction.PackageRequirements
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class BlindingSpec extends AnyFreeSpec with Matchers {

  import TestNodeBuilder.CreateKey
  import TransactionBuilder.Implicits._

  def create(builder: TxBuilder): (Value.ContractId, Node.Create) = {
    val cid = builder.newCid
    val create = builder.create(
      id = cid,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.Empty),
      signatories = Seq("Alice", "Bob"),
      observers = Seq("Carl"),
    )
    (cid, create)
  }

  "blind" - {
    // TEST_EVIDENCE: Confidentiality: ensure correct privacy for create node
    "create" in {
      val builder = new TxBuilder()
      val (_, createNode) = create(builder)
      val nodeId = builder.add(createNode)
      val tx = builder.build()
      val blindingInfo = Blinding.blind(tx)
      blindingInfo shouldBe BlindingInfo(
        disclosure = Map(nodeId -> Set("Alice", "Bob", "Carl")),
        divulgence = Map.empty,
      )
    }
    // TEST_EVIDENCE: Confidentiality: ensure correct privacy for exercise node (consuming)
    "consuming exercise" in {
      val builder = new TxBuilder()
      val (cid, createNode) = create(builder)
      val exercise = builder.exercise(
        createNode,
        "C",
        consuming = true,
        Set("Actor"),
        ValueRecord(None, ImmArray.Empty),
        choiceObservers = Set("ChoiceObserver"),
        byKey = false,
      )
      val nodeId = builder.add(exercise)
      val tx = builder.build()
      val blindingInfo = Blinding.blind(tx)
      blindingInfo shouldBe BlindingInfo(
        disclosure = Map(nodeId -> Set("ChoiceObserver", "Carl", "Bob", "Actor", "Alice")),
        divulgence = Map(cid -> Set("ChoiceObserver")),
      )
    }
    // TEST_EVIDENCE: Confidentiality: ensure correct privacy for exercise node (non-consuming)
    "non-consuming exercise" in {
      val builder = new TxBuilder()
      val (cid, createNode) = create(builder)
      val exercise = builder.exercise(
        createNode,
        "C",
        consuming = false,
        Set("Actor"),
        ValueRecord(None, ImmArray.empty),
        choiceObservers = Set("ChoiceObserver"),
        byKey = false,
      )
      val nodeId = builder.add(exercise)
      val tx = builder.build()
      val blindingInfo = Blinding.blind(tx)
      blindingInfo shouldBe BlindingInfo(
        disclosure = Map(nodeId -> Set("ChoiceObserver", "Bob", "Actor", "Alice")),
        divulgence = Map(cid -> Set("ChoiceObserver")),
      )
    }
  }
  // TEST_EVIDENCE: Confidentiality: ensure correct privacy for fetch node
  "fetch" in {
    val builder = new TxBuilder()
    val (_, createNode) = create(builder)
    val fetch = builder.fetch(createNode, byKey = false)
    val nodeId = builder.add(fetch)
    val tx = builder.build()
    val blindingInfo = Blinding.blind(tx)
    blindingInfo shouldBe BlindingInfo(
      disclosure = Map(nodeId -> Set("Bob", "Alice")),
      divulgence = Map.empty,
    )
  }
  // TEST_EVIDENCE: Confidentiality: ensure correct privacy for lookup-by-key node (found)
  "lookupByKey found" in {
    val builder = new TxBuilder()
    val cid = builder.newCid
    val create = builder.create(
      id = cid,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("Alice", "Bob"),
      observers = Seq("Carl"),
      key = CreateKey.KeyWithMaintainers(ValueRecord(None, ImmArray.empty), Seq("Alice")),
    )
    val lookup = builder.lookupByKey(create)
    val nodeId = builder.add(lookup)
    val tx = builder.build()
    val blindingInfo = Blinding.blind(tx)
    blindingInfo shouldBe BlindingInfo(
      disclosure = Map(nodeId -> Set("Alice")),
      divulgence = Map.empty,
    )
  }
  // TEST_EVIDENCE: Confidentiality: ensure correct privacy for lookup-by-key node (not-found)
  "lookupByKey not found" in {
    val builder = new TxBuilder()
    val cid = builder.newCid
    val create = builder.create(
      id = cid,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("Alice", "Bob"),
      observers = Seq("Carl"),
      key = CreateKey.KeyWithMaintainers(ValueRecord(None, ImmArray.empty), Seq("Alice")),
    )
    val lookup = builder.lookupByKey(create, found = false)
    val nodeId = builder.add(lookup)
    val tx = builder.build()
    val blindingInfo = Blinding.blind(tx)
    blindingInfo shouldBe BlindingInfo(
      disclosure = Map(nodeId -> Set("Alice")),
      divulgence = Map.empty,
    )
  }

  // TEST_EVIDENCE: Confidentiality: ensure correct privacy for exercise subtree
  "exercise with children" in {
    val builder = new TxBuilder()
    val cid1 = builder.newCid
    val cid2 = builder.newCid
    val cid3 = builder.newCid
    val cid4 = builder.newCid
    val create1 = builder.create(
      id = cid1,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("A"),
      observers = Seq(),
    )
    val create2 = builder.create(
      id = cid2,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("B"),
      observers = Seq(),
    )
    val create3 = builder.create(
      id = cid3,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("C"),
      observers = Seq(),
    )
    val create4 = builder.create(
      id = cid4,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("D"),
      observers = Seq(),
    )
    val ex1 =
      builder.add(
        builder.exercise(
          create1,
          "C",
          consuming = true,
          Set("A"),
          ValueRecord(None, ImmArray.empty),
          byKey = false,
        )
      )
    val c2Id = builder.add(create2, ex1)
    val ex2 = builder.add(
      builder.exercise(
        create2,
        "C",
        consuming = true,
        Set("B"),
        ValueRecord(None, ImmArray.empty),
        byKey = false,
      ),
      ex1,
    )
    val c3Id = builder.add(create3, ex2)
    val c4Id = builder.add(create4, ex2)
    val tx = builder.build()
    val blindingInfo = Blinding.blind(tx)
    blindingInfo shouldBe BlindingInfo(
      disclosure = Map(
        ex1 -> Set("A"),
        c2Id -> Set("A", "B"),
        ex2 -> Set("A", "B"),
        c3Id -> Set("A", "B", "C"),
        c4Id -> Set("A", "B", "D"),
      ),
      divulgence = Map(
        create2.coid -> Set("A")
      ),
    )
  }
  // TEST_EVIDENCE: Confidentiality: ensure correct privacy for rollback subtree
  "rollback" in {
    val builder = new TxBuilder()
    val cid1 = builder.newCid
    val cid2 = builder.newCid
    val create1 = builder.create(
      id = cid1,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("A", "B"),
      observers = Seq(),
    )
    val ex1 = builder.add(
      builder.exercise(
        create1,
        "Choice",
        consuming = true,
        Set("C"),
        ValueRecord(None, ImmArray.empty),
        byKey = false,
      )
    )
    val rollback = builder.add(builder.rollback(), ex1)
    val create2 = builder.create(
      id = cid2,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("D"),
      observers = Seq(),
    )
    val create2Node = builder.add(create2, rollback)
    val ex2 = builder.add(
      builder
        .exercise(
          contract = create2,
          "Choice",
          consuming = true,
          Set("F"),
          ValueRecord(None, ImmArray.empty),
          byKey = false,
        ),
      rollback,
    )
    val blindingInfo = Blinding.blind(builder.build())
    blindingInfo shouldBe BlindingInfo(
      disclosure = Map(
        ex1 -> Set("A", "B", "C"),
        rollback -> Set("A", "B", "C"),
        create2Node -> Set("A", "B", "C", "D"),
        ex2 -> Set("A", "B", "C", "D", "F"),
      ),
      divulgence = Map(
        cid2 -> Set("A", "B", "C")
      ),
    )
  }

  "contract visibility" in {
    val (_, visibility) = BlindingTransaction.calculateBlindingInfoWithContractVisibility(
      BlindingSpec.sampleTxForPartyPackagesVisibility
    )

    visibility shouldBe Map(
      createC1.coid -> Set(sig1, act),
      createC2.coid -> Set(sig1, sig2, act),
      createC3.coid -> Set(sig1, sig3, sig4, act),
      createC4.coid -> Set(sig1, sig4, act),
    )
  }

  "party package requirements" in {
    val inputContractsPackages =
      Map(createC1.coid -> pkgIn1, createC2.coid -> pkgIn2, createC3.coid -> pkg3)
    val packageRequirementsPerParty =
      Blinding.partyPackageRequirements(
        BlindingSpec.sampleTxForPartyPackagesVisibility,
        inputContractsPackages,
      )

    packageRequirementsPerParty shouldBe Map(
      sig1 -> PackageRequirements(
        checkOnly = Set(pkgIn1, pkgIn2),
        vetted = Set(pkgIface1, pkgIface2, pkg2, pkg3, pkg4),
      ),
      sig2 -> PackageRequirements(checkOnly = Set(pkgIn2), vetted = Set(pkg2)),
      sig3 -> PackageRequirements(checkOnly = Set(pkg3), vetted = Set(pkgIface2, pkg2)),
      sig4 -> PackageRequirements(
        checkOnly = Set.empty,
        vetted = Set(pkgIface1, pkgIface2, pkg1, pkg2, pkg3, pkg4),
      ),
      act -> PackageRequirements(
        checkOnly = Set(pkgIn1, pkgIn2),
        vetted = Set(pkgIface1, pkgIface2, pkg2, pkg3, pkg4),
      ),
    )
  }

  "divulge created contracts" in {

    val templateId = pkg1 | defaultQualifiedName
    val arg = ValueRecord(None, ImmArray.Empty)

    val builder = new TxBuilder with TestIdFactory

    val create0 = builder.create(
      id = builder.newCid,
      templateId = templateId,
      argument = ValueRecord(None, ImmArray.Empty),
      signatories = Set(sig1),
    )

    val create1 = builder.create(
      id = builder.newCid,
      templateId = templateId,
      argument = arg,
      signatories = Set(sig2),
    )

    val exercise0 = builder
      .exercise(
        contract = create0,
        choice = "C",
        consuming = false,
        actingParties = Set(sig3),
        argument = arg,
        byKey = false,
      )
      .withChildren(create1)

    val tx = TreeTransactionBuilder.toVersionedTransaction(exercise0)

    val (_, visibility) = BlindingTransaction.calculateBlindingInfoWithContractVisibility(tx)
    visibility.get(create1.coid) shouldBe Some(Set(sig1, sig2, sig3))
  }

}

object BlindingSpec {
  class TxBuilder extends NodeIdTransactionBuilder with TestNodeBuilder {
    val defaultPackageName = Some(Ref.PackageName.assertFromString("-default-"))
  }

  import TransactionBuilder.Implicits.{toName, toParties}

  implicit class PkgToTypeConName(pkg: Ref.PackageId) {
    def |(shortTypeConName: String) =
      Ref.TypeConName(pkg, Ref.QualifiedName.assertFromString(shortTypeConName))
  }

  // For the purpose of testing the partyPackageRequirements, we don't care about the qualified name of identifiers
  // hence use the same default one for brevity
  private val defaultQualifiedName = "M:T"

  private def create(
      pkgId: Ref.PackageId,
      sigs: Seq[Ref.Party],
      obs: Seq[Ref.Party] = Seq.empty,
      key: CreateKey = CreateKey.NoKey,
  ) =
    builder.create(
      id = builder.newCid,
      templateId = pkgId | defaultQualifiedName,
      argument = ValueRecord(None, ImmArray.empty),
      signatories = sigs,
      observers = obs,
      key = key,
    )

  private def exercise(
      create: Node.Create,
      pkgId: Ref.PackageId,
      acts: Seq[Ref.Party],
      interfacePkgId: Option[Ref.PackageId] = None,
  ): Node.Exercise =
    builder
      .exercise(
        contract = create,
        choice = "C",
        consuming = true,
        actingParties = acts,
        argument = ValueRecord(None, ImmArray.empty),
        byKey = false,
      )
      .copy(
        templateId = pkgId | defaultQualifiedName,
        interfaceId = interfacePkgId.map(_ | defaultQualifiedName),
      )

  private val builder = new TxBuilder()

  private val Seq(sig1, sig2, sig3, sig4) =
    (1 to 4).map(idx => Ref.Party.assertFromString(s"sig$idx"))
  private val act = Ref.Party.assertFromString("act")

  private val Seq(pkgIface1, pkgIface2) =
    (1 to 2).map(idx => Ref.PackageId.assertFromString(s"iface$idx"))
  private val Seq(pkgIn1, pkgIn2) =
    (1 to 2).map(idx => Ref.PackageId.assertFromString(s"pkgIn$idx"))
  private val Seq(pkg1, pkg2, pkg3, pkg4) =
    (1 to 4).map(idx => Ref.PackageId.assertFromString(s"pkg$idx"))

  private val createC1 = create(pkgIn1, Seq(sig1))
  private val createC2 = create(pkgIn2, Seq(sig2))
  private val createC3 =
    create(pkg3, Seq(sig3, sig4), key = CreateKey.KeyWithMaintainers(ValueTrue, Set(sig4)))
  private val createC4 = create(pkg1, Seq(sig4))

  private val fetchC2 =
    builder.fetch(createC2, byKey = false).copy(templateId = pkg2 | defaultQualifiedName)

  private val exeC1 = exercise(createC1, pkg2, acts = Seq(act))

  private val iExeC4 = exercise(createC4, pkg4, acts = Seq(sig4), interfacePkgId = Some(pkgIface1))

  private val iFetchC3 = builder
    .fetch(createC3, byKey = true)
    .copy(templateId = pkg2 | "M:T", interfaceId = Some(pkgIface2 | defaultQualifiedName))

  private val lbkC3 = builder.lookupByKey(createC3)

  /*
  Input contracts
  =====
  - contract C1 created with pkgIn1 by sig1
  - contract C2 created with pkgIn2 by sig2
  - contract C3 created with pkg3 by sig3

  Transaction with two roots
  ======================
  Create C4       Exe C1
   (pkg1)         (pkg2)
     |              |
                    ------------------------------------------------
                    |            |              |                  |
                 Fetch C2      LBK C3        IExe C4            IFetch C3
                  (pkg2)       (pkg3)     (pkg4-pkgIface)    (pkg2-pkgIface2)

   NOTE: Not well-authorized.
         What matters here is the visibility of each node, contract and package
   */
  private val sampleTxForPartyPackagesVisibility: VersionedTransaction =
    TreeTransactionBuilder.toVersionedTransaction(
      createC4,
      exeC1.withChildren(fetchC2, lbkC3, iExeC4, iFetchC3),
    )
}
