// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.transaction.test.TestNodeBuilder.CreateKey
import com.daml.lf.transaction.test.TestNodeBuilder.CreateKey.NoKey
import com.daml.lf.crypto.Hash
import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.{Bytes, ImmArray, Ref}
import com.daml.lf.transaction.Transaction.{
  AliasedNode,
  ChildrenRecursion,
  DanglingNodeId,
  NotWellFormedError,
  OrphanedNode,
}
import com.daml.lf.transaction.TransactionErrors.{
  DuplicateContractId,
  DuplicateContractIdKIError,
  DuplicateContractKey,
  DuplicateContractKeyKIError,
  InconsistentContractKey,
  KeyInputError,
}
import com.daml.lf.transaction.test.{NodeIdTransactionBuilder, TestNodeBuilder, TransactionBuilder}
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.test.ValueGenerators.danglingRefGenNode
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec

import scala.collection.immutable.HashMap
import scala.util.Random
import NodeVersionUpdater.Ops

class TransactionSpec
    extends AnyFreeSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {

  import TransactionSpec._
  import TransactionBuilder.Implicits._

  "isWellFormed" - {
    "detects dangling references in roots" in {
      val tx = mkTransaction(HashMap.empty, ImmArray(NodeId(1)))
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(1), DanglingNodeId))
    }

    "detects dangling references in children" in {
      val tx = mkTransaction(
        HashMap(NodeId(1) -> dummyExerciseNode(cid("#cid1"), ImmArray(NodeId(2)))),
        ImmArray(NodeId(1)),
      )
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(2), DanglingNodeId))
    }

    "detects cycles" in {
      val tx = mkTransaction(
        HashMap(NodeId(1) -> dummyExerciseNode(cid("#cid1"), ImmArray(NodeId(1)))),
        ImmArray(NodeId(1)),
      )
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(1), AliasedNode))
    }

    "detects aliasing from roots and exercise" in {
      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyExerciseNode(cid("#cid0"), ImmArray(NodeId(1))),
          NodeId(1) -> dummyExerciseNode(cid("#cid1"), ImmArray(NodeId(2))),
          NodeId(2) -> dummyCreateNode(cid("#cid2")),
        ),
        ImmArray(NodeId(0), NodeId(2)),
      )
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(2), AliasedNode))
    }

    "detects orphans" in {
      val tx = mkTransaction(HashMap(NodeId(1) -> dummyCreateNode(cid("#cid1"))), ImmArray.Empty)
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(1), OrphanedNode))
    }
  }

  "cids" - {

    "collects contract IDs" in {
      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyExerciseNode(cid("#cid0"), ImmArray(NodeId(1))),
          NodeId(1) -> dummyExerciseNode(cid("#cid1"), ImmArray(NodeId(2))),
          NodeId(2) -> dummyCreateNode(cid("#cid2")),
        ),
        ImmArray(NodeId(0), NodeId(2)),
      )

      def collectCids(tx: Transaction): Set[V.ContractId] = {
        val cids = Set.newBuilder[V.ContractId]
        tx.foreachCid(cids += _)
        cids.result()
      }

      collectCids(tx) shouldBe Set[V.ContractId](
        cid("#cid0"),
        cid("#cid1"),
        cid("#cid2"),
        cid("#dummyCid"),
      )

    }

  }

  "foldInExecutionOrder" - {
    "should traverse the transaction in execution order" in {

      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyCreateNode(cid("#cid0")),
          NodeId(1) -> dummyExerciseNode(cid("#cid1"), ImmArray(NodeId(2), NodeId(4))),
          NodeId(2) -> dummyExerciseNode(cid("#cid2"), ImmArray.Empty),
          NodeId(3) -> dummyCreateNode(cid("#cid3")),
          NodeId(4) -> dummyRollbackNode(ImmArray(NodeId(5))),
          NodeId(5) -> dummyCreateNode(cid("#cid5")),
        ),
        ImmArray(NodeId(0), NodeId(1), NodeId(3)),
      )

      val result = tx.foldInExecutionOrder(List.empty[String])(
        exerciseBegin =
          (acc, nid, _) => (s"exerciseBegin(${nid.index})" :: acc, ChildrenRecursion.DoRecurse),
        rollbackBegin =
          (acc, nid, _) => (s"rollbackBegin(${nid.index})" :: acc, ChildrenRecursion.DoRecurse),
        leaf = (acc, nid, _) => s"leaf(${nid.index})" :: acc,
        exerciseEnd = (acc, nid, _) => s"exerciseEnd(${nid.index})" :: acc,
        rollbackEnd = (acc, nid, _) => s"rollbackEnd(${nid.index})" :: acc,
      )

      result.reverse.mkString(", ") shouldBe
        "leaf(0), exerciseBegin(1), exerciseBegin(2), exerciseEnd(2), rollbackBegin(4), leaf(5), rollbackEnd(4), exerciseEnd(1), leaf(3)"
    }
  }

  "reachableNodeIds" - {
    "should collect the node-ids reachable from the roots" in {

      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyCreateNode(cid("#cid0")),
          NodeId(1) -> dummyExerciseNode(cid("#cid1"), ImmArray(NodeId(2), NodeId(4))),
          NodeId(2) -> dummyExerciseNode(cid("#cid2"), ImmArray.Empty),
          NodeId(3) -> dummyCreateNode(cid("#cid3")),
          NodeId(4) -> dummyRollbackNode(ImmArray(NodeId(5))),
          NodeId(5) -> dummyCreateNode(cid("#cid5")),
          // these are not reachable
          NodeId(10) -> dummyCreateNode(cid("#cid10")),
          NodeId(11) -> dummyExerciseNode(cid("#cid11"), ImmArray(NodeId(12), NodeId(14))),
          NodeId(12) -> dummyExerciseNode(cid("#cid12"), ImmArray.Empty),
          NodeId(13) -> dummyCreateNode(cid("#cid13")),
          NodeId(14) -> dummyRollbackNode(ImmArray(NodeId(15))),
          NodeId(15) -> dummyCreateNode(cid("#cid15")),
        ),
        ImmArray(NodeId(0), NodeId(1), NodeId(3)),
      )

      val result: Set[Int] = tx.reachableNodeIds.map(_.index)
      result shouldBe Set(0, 1, 2, 3, 4, 5)
    }
  }

  /* TODO SC Gen for well-formed Transaction needed first
  "equalForest" - {
    "is reflexive" in forAll(genTransaction) { tx =>
      tx equalForest tx shouldBe true
    }

    "is node-id-parametric" in forAll(genTransaction) { tx =>
      tx mapNodeId ((_, ())) equalForest tx shouldBe true
    }

    "negation implies == negation" in forAll(genTransaction, genTransaction) { (txa, txb) =>
      whenever(!(txa equalForest txb)) {
        txa should not be txb
      }
    }
  }
   */

  "isReplayedBy" - {
    def genTrans(node: Node) = {
      val nid = NodeId(1)
      val version = node.optVersion.getOrElse(TransactionVersion.minVersion)
      VersionedTransaction(version, HashMap(nid -> node), ImmArray(nid))
    }

    def isReplayedBy(
        n1: Node,
        n2: Node,
    ) = Validation.isReplayedBy(genTrans(n1), genTrans(n2))

    // the whole-transaction-relevant parts are handled by equalForest testing
    val genEmptyNode: Gen[Node] =
      for {
        entry <- danglingRefGenNode
        node = entry match {
          case (_, nr: Node.Rollback) =>
            nr.copy(children = ImmArray.Empty)
          case (_, n: Node.LeafOnlyAction) => n
          case (_, ne: Node.Exercise) =>
            ne.copy(children = ImmArray.Empty)
        }
      } yield node

    "is reflexive" in forAll(genEmptyNode) { n =>
      val tx = Normalization.normalizeTx(genTrans(n))
      Validation.isReplayedBy(tx, tx) shouldBe Right(())
    }

    "fail if version is different" in {
      val versions = TransactionVersion.All

      def diffVersion(v: TransactionVersion) = {
        val randomVersion = versions(Random.nextInt(versions.length - 1))
        if (randomVersion != v) randomVersion else versions.last
      }

      forAll(genEmptyNode, minSuccessful(10)) { n =>
        val version = n.optVersion.getOrElse(TransactionVersion.minVersion)
        n match {
          case _: Node.Rollback => ()
          case n: Node.Action =>
            val m = n.updateVersion(diffVersion(version))
            isReplayedBy(n, m) shouldBe Symbol("left")
        }
      }
    }

    "negation implies == negation" in forAll(genEmptyNode, genEmptyNode) { (na, nb) =>
      whenever(isReplayedBy(na, nb).isLeft) {
        na should not be nb
      }
    }
  }

  "suffixCid" - {
    "suffix non suffixed and only non suffixed contract ids" in {

      val cids = List.fill(2)(TransactionBuilder.newV1Cid)
      assert(cids.distinct.length == cids.length)
      val List(cid1, cid2) = cids
      val mapping1 = cids.map { cid =>
        assert(cid.suffix.isEmpty)
        cid.discriminator -> cid.discriminator.bytes.slice(10, 20)
      }.toMap
      val mapping2: V.ContractId => V.ContractId = {
        case cid @ V.ContractId.V1(discriminator, Bytes.Empty) =>
          mapping1.get(discriminator) match {
            case Some(value) => V.ContractId.V1.assertBuild(discriminator, value)
            case None => cid
          }
        case cid => cid
      }

      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyCreateNode(cid1),
          NodeId(0) -> dummyExerciseNode(cid1, ImmArray(NodeId(0)), true),
          NodeId(1) -> dummyExerciseNode(cid2, ImmArray(NodeId(1)), true),
        ),
        ImmArray(NodeId(0), NodeId(1)),
      )

      val tx1 = tx.suffixCid(mapping1)
      val tx2 = tx.suffixCid(mapping1)

      tx1 shouldNot be(tx)
      tx2 shouldBe tx1
      tx1 shouldBe Right(tx.mapCid(mapping2))

    }
  }

  "contractKeys" - {
    "return all the contract keys" in {

      val builder = new TxBuilder()
      val parties = Set("Alice")

      def create(s: V.ContractId) = {
        println(s)
        builder
          .create(
            id = s,
            templateId = s"Mod:t${s.coid}",
            argument = V.ValueUnit,
            signatories = parties,
            observers = parties,
            key = CreateKey.SignatoryMaintainerKey(V.ValueText(s.coid)),
          )
      }

      def exe(s: V.ContractId, consuming: Boolean, byKey: Boolean) =
        builder
          .exercise(
            contract = create(s),
            choice = s"Choice${s.coid}",
            actingParties = parties.toSet,
            consuming = consuming,
            argument = V.ValueUnit,
            byKey = byKey,
          )

      def fetch(s: V.ContractId, byKey: Boolean) =
        builder.fetch(contract = create(s), byKey = byKey)

      def lookup(s: V.ContractId, found: Boolean) =
        builder.lookupByKey(contract = create(s), found = found)

      val root1 =
        builder.create(
          cid("#root"),
          templateId = "Mod:Root",
          argument = V.ValueUnit,
          signatories = parties,
          observers = parties,
        )

      val root2 = builder.exercise(
        root1,
        "ExerciseRoot",
        actingParties = parties.toSet,
        consuming = true,
        argument = V.ValueUnit,
        byKey = false,
      )

      builder.add(root1)
      val exeId = builder.add(root2)
      println(cid("Create"))
      builder.add(create(cid("Create"): V.ContractId))
      println("created")
      builder.add(exe(cid("NonConsumingExerciseById"), false, false), exeId)
      builder.add(exe(cid("ConsumingExerciseById"), true, false), exeId)
      builder.add(exe(cid("NonConsumingExerciseByKey"), false, true), exeId)
      builder.add(exe(cid("NonConsumingExerciseByKey"), true, true), exeId)
      builder.add(fetch(cid("FetchById"), false), exeId)
      builder.add(fetch(cid("FetchByKey"), true), exeId)
      builder.add(lookup(cid("SuccessfulLookup"), true), exeId)
      builder.add(lookup(cid("UnsuccessfulLookup"), true), exeId)
      val rollbackId = builder.add(Node.Rollback(ImmArray.Empty))
      builder.add(create(cid("RolledBackCreate")))
      builder.add(exe(cid("RolledBackNonConsumingExerciseById"), false, false), rollbackId)
      builder.add(exe(cid("RolledBackConsumingExerciseById"), true, false), rollbackId)
      builder.add(exe(cid("RolledBackNonConsumingExerciseByKey"), false, true), rollbackId)
      builder.add(exe(cid("RolledBackNonConsumingExerciseByKey"), true, true), rollbackId)
      builder.add(fetch(cid("RolledBackFetchById"), false), rollbackId)
      builder.add(fetch(cid("RolledBackFetchByKey"), true), rollbackId)
      builder.add(lookup(cid("RolledBackSuccessfulLookup"), true), rollbackId)
      builder.add(lookup(cid("RolledBackUnsuccessfulLookup"), true), rollbackId)

      val expectedResults =
        Iterator(
          "Create",
          "NonConsumingExerciseById",
          "ConsumingExerciseById",
          "NonConsumingExerciseByKey",
          "NonConsumingExerciseByKey",
          "FetchById",
          "FetchByKey",
          "SuccessfulLookup",
          "UnsuccessfulLookup",
          "RolledBackCreate",
          "RolledBackNonConsumingExerciseById",
          "RolledBackConsumingExerciseById",
          "RolledBackNonConsumingExerciseByKey",
          "RolledBackNonConsumingExerciseByKey",
          "RolledBackFetchById",
          "RolledBackFetchByKey",
          "RolledBackSuccessfulLookup",
          "RolledBackUnsuccessfulLookup",
        ).map(s => {
          val node = create(cid(s))
          GlobalKey
            .assertBuild(
              node.templateId,
              V.ValueText(cid(s).coid),
              KeyPackageName(node.packageName, node.version),
            )
        }).toSet

      builder.build().contractKeys shouldBe expectedResults
    }
  }

  "contractKeyInputs" - {
    import Transaction._
    val dummyBuilder = new TxBuilder()
    val parties = List("Alice")
    def keyValue(s: String) = V.ValueText(s)
    def globalKey(
        k: String,
        pkgName: Option[Ref.PackageName] = None,
        version: TransactionVersion = TransactionVersion.minVersion,
    ) =
      GlobalKey.assertBuild("Mod:T", keyValue(k), KeyPackageName(pkgName, version))
    def create(s: V.ContractId, k: String) = dummyBuilder
      .create(
        id = s,
        templateId = "Mod:T",
        argument = V.ValueUnit,
        signatories = parties,
        observers = parties,
        key = CreateKey.SignatoryMaintainerKey(keyValue(k)),
      )

    def exe(s: V.ContractId, k: String, consuming: Boolean, byKey: Boolean) =
      dummyBuilder
        .exercise(
          contract = create(s, k),
          choice = "Choice",
          actingParties = parties.toSet,
          consuming = consuming,
          argument = V.ValueUnit,
          byKey = byKey,
        )

    def fetch(s: V.ContractId, k: String, byKey: Boolean) =
      dummyBuilder.fetch(contract = create(s, k), byKey = byKey)

    def lookup(s: V.ContractId, k: String, found: Boolean) =
      dummyBuilder.lookupByKey(contract = create(s, k), found = found)

    "return None for create" in {
      val builder = new TxBuilder()
      val createNode = create(cid("#0"), "k0")
      builder.add(createNode)
      builder.build().contractKeyInputs shouldBe Right(
        Map(globalKey("k0") -> KeyCreate)
      )
    }
    "return Some(_) for fetch and fetch-by-key" in {
      val builder = new TxBuilder()
      val fetchNode0 = fetch(cid("#0"), "k0", byKey = false)
      val fetchNode1 = fetch(cid("#1"), "k1", byKey = true)
      builder.add(fetchNode0)
      builder.add(fetchNode1)
      builder.build().contractKeyInputs shouldBe Right(
        Map(
          globalKey("k0") -> KeyActive(cid("#0")),
          globalKey("k1") -> KeyActive(cid("#1")),
        )
      )
    }
    "return Some(_) for consuming/non-consuming exercise and exercise-by-key" in {
      val builder = new TxBuilder()
      val exe0 = exe(cid("#0"), "k0", consuming = false, byKey = false)
      val exe1 = exe(cid("#1"), "k1", consuming = true, byKey = false)
      val exe2 = exe(cid("#2"), "k2", consuming = false, byKey = true)
      val exe3 = exe(cid("#3"), "k3", consuming = true, byKey = true)
      builder.add(exe0)
      builder.add(exe1)
      builder.add(exe2)
      builder.add(exe3)
      builder.build().contractKeyInputs shouldBe Right(
        Map(
          globalKey("k0") -> KeyActive(cid("#0")),
          globalKey("k1") -> KeyActive(cid("#1")),
          globalKey("k2") -> KeyActive(cid("#2")),
          globalKey("k3") -> KeyActive(cid("#3")),
        )
      )
    }

    "return None for negative lookup by key" in {
      val builder = new TxBuilder()
      val lookupNode = lookup(cid("#0"), "k0", found = false)
      builder.add(lookupNode)
      builder.build().contractKeyInputs shouldBe Right(
        Map(globalKey("k0") -> NegativeKeyLookup)
      )
    }

    "return Some(_) for positive lookup by key" in {
      val builder = new TxBuilder()
      val lookupNode = lookup(cid("#0"), "k0", found = true)
      builder.add(lookupNode)
      inside(lookupNode.result) { case Some(contractId) =>
        contractId shouldBe cid("#0")
        builder.build().contractKeyInputs shouldBe Right(
          Map(globalKey("k0") -> KeyActive(contractId))
        )
      }
    }
    "returns keys used under rollback nodes" in {
      val builder = new TxBuilder()
      val createNode = create(cid("#0"), "k0")
      val exerciseNode = exe(cid("#1"), "k1", consuming = false, byKey = false)
      val fetchNode = fetch(cid("#2"), "k2", byKey = false)
      val lookupNode = lookup(cid("#3"), "k3", found = false)
      val rollback = builder.add(builder.rollback())
      builder.add(createNode, rollback)
      builder.add(exerciseNode, rollback)
      builder.add(fetchNode, rollback)
      builder.add(lookupNode, rollback)
      builder.build().contractKeyInputs shouldBe Right(
        Map(
          globalKey("k0") -> KeyCreate,
          globalKey("k1") -> KeyActive(exerciseNode.targetCoid),
          globalKey("k2") -> KeyActive(fetchNode.coid),
          globalKey("k3") -> NegativeKeyLookup,
        )
      )
    }
    "fetch and create conflict for the same contract ID" in {
      val builder = new TxBuilder()
      builder.add(fetch(cid("#0"), "k0", byKey = false))
      builder.add(create(cid("#0"), "k1"))
      builder.build().contractKeyInputs shouldBe Left(
        DuplicateContractIdKIError(DuplicateContractId(cid("#0")))
      )
    }
    "lookup by key and create conflict for the same contract ID" in {
      val builder = new TxBuilder()
      builder.add(lookup(cid("#0"), "k0", found = true))
      builder.add(create(cid("#0"), "k1"))
      builder.build().contractKeyInputs shouldBe Left(
        DuplicateContractIdKIError(DuplicateContractId(cid("#0")))
      )
    }
    "consuming exercise and create conflict for the same contract ID" in {
      val builder = new TxBuilder()
      builder.add(exe(cid("#0"), "k1", consuming = true, byKey = false))
      builder.add(create(cid("#0"), "k2"))
      builder.build().contractKeyInputs shouldBe Left(
        DuplicateContractIdKIError(DuplicateContractId(cid("#0")))
      )
    }
    "non-consuming exercise and create conflict for the same contract ID" in {
      val builder = new TxBuilder()
      builder.add(exe(cid("#0"), "k1", consuming = false, byKey = false))
      builder.add(create(cid("#0"), "k2"))
      builder.build().contractKeyInputs shouldBe Left(
        DuplicateContractIdKIError(DuplicateContractId(cid("#0")))
      )
    }
    "two creates conflict" in {
      val builder = new TxBuilder()
      builder.add(create(cid("#0"), "k0"))
      builder.add(create(cid("#1"), "k0"))
      builder.build().contractKeyInputs shouldBe Left(
        DuplicateContractKeyKIError(DuplicateContractKey(globalKey("k0")))
      )
    }
    "two creates do not conflict if interleaved with archive" in {
      val builder = new TxBuilder()
      builder.add(create(cid("#0"), "k0"))
      builder.add(exe(cid("#0"), "k0", consuming = true, byKey = false))
      builder.add(create(cid("#1"), "k0"))
      builder.build().contractKeyInputs shouldBe Right(Map(globalKey("k0") -> KeyCreate))
    }
    "two creates do not conflict if one is in rollback" in {
      val builder = new TxBuilder()
      val rollback = builder.add(builder.rollback())
      builder.add(create(cid("#0"), "k0"), rollback)
      builder.add(create(cid("#1"), "k0"))
      builder.build().contractKeyInputs shouldBe Right(Map(globalKey("k0") -> KeyCreate))
    }
    "negative lookup after create fails" in {
      val builder = new TxBuilder()
      builder.add(create(cid("#0"), "k0"))
      builder.add(lookup(cid("#0"), "k0", found = false))
      builder.build().contractKeyInputs shouldBe Left(
        KeyInputError.inject(InconsistentContractKey(globalKey("k0")))
      )
    }
    "inconsistent lookups conflict" in {
      val builder = new TxBuilder()
      builder.add(lookup(cid("#0"), "k0", found = true))
      builder.add(lookup(cid("#0"), "k0", found = false))
      builder.build().contractKeyInputs shouldBe Left(
        KeyInputError.inject(InconsistentContractKey(globalKey("k0")))
      )
    }
    "inconsistent lookups conflict across rollback" in {
      val builder = new TxBuilder()
      val rollback = builder.add(builder.rollback())
      builder.add(lookup(cid("#0"), "k0", found = true), rollback)
      builder.add(lookup(cid("#0"), "k0", found = false))
      builder.build().contractKeyInputs shouldBe Left(
        KeyInputError.inject(InconsistentContractKey(globalKey("k0")))
      )
    }
    "positive lookup conflicts with create" in {
      val builder = new TxBuilder()
      builder.add(lookup(cid("#0"), "k0", found = true))
      builder.add(create(cid("#1"), "k0"))
      builder.build().contractKeyInputs shouldBe Left(
        KeyInputError.inject(DuplicateContractKey(globalKey("k0")))
      )
    }
    "positive lookup in rollback conflicts with create" in {
      val builder = new TxBuilder()
      val rollback = builder.add(builder.rollback())
      builder.add(lookup(cid("#0"), "k0", found = true), rollback)
      builder.add(create(cid("#1"), "k0"))
      builder.build().contractKeyInputs shouldBe Left(
        KeyInputError.inject(DuplicateContractKey(globalKey("k0")))
      )
    }
    "rolled back archive does not prevent conflict" in {
      val builder = new TxBuilder()
      builder.add(create(cid("#0"), "k0"))
      val rollback = builder.add(builder.rollback())
      builder.add(exe(cid("#0"), "k0", consuming = true, byKey = true), rollback)
      builder.add(create(cid("#1"), "k0"))
      builder.build().contractKeyInputs shouldBe Left(
        KeyInputError.inject(DuplicateContractKey(globalKey("k0")))
      )
    }
    "successful, inconsistent lookups conflict" in {
      val builder = new TxBuilder()
      val create0 = create(cid("#0"), "k0")
      val create1 = create(cid("#1"), "k0")
      builder.add(builder.lookupByKey(create0, found = true))
      builder.add(builder.lookupByKey(create1, found = true))
      builder.build().contractKeyInputs shouldBe Left(
        KeyInputError.inject(InconsistentContractKey(globalKey("k0")))
      )
    }
    "first negative input wins" in {
      val builder = new TxBuilder()
      val rollback = builder.add(builder.rollback())
      val create0 = create(cid("#0"), "k0")
      val lookup0 = builder.lookupByKey(create0, found = false)
      val create1 = create(cid("#1"), "k1")
      val lookup1 = builder.lookupByKey(create1, found = false)
      builder.add(create0, rollback)
      builder.add(lookup1, rollback)
      builder.add(lookup0)
      builder.add(create1)
      builder.build().contractKeyInputs shouldBe Right(
        Map(globalKey("k0") -> KeyCreate, globalKey("k1") -> NegativeKeyLookup)
      )
    }
  }

  def create(
      builder: TxBuilder,
      parties: Set[Ref.Party],
      key: Option[String] = None,
  ): (V.ContractId, Node.Create) = {
    val cid = builder.newCid
    val node = builder.create(
      id = cid,
      templateId = "Mod:T",
      argument = V.ValueUnit,
      signatories = parties,
      observers = Seq(),
      key = key.fold[CreateKey](NoKey)(s => CreateKey.SignatoryMaintainerKey(V.ValueText(s))),
    )
    (cid, node)
  }
  def exercise(
      builder: TxBuilder,
      create: Node.Create,
      parties: Set[Ref.Party],
      consuming: Boolean,
  ): Node.Exercise =
    builder.exercise(
      contract = create,
      choice = "C",
      actingParties = parties.toSet,
      consuming = consuming,
      argument = V.ValueUnit,
      byKey = false,
    )

  val activenessTest = {}

  "consumedContracts and inactiveContracts" - {
    val builder = new TxBuilder()
    val parties = Seq("Alice")
    val (cid0, create0) = create(builder, parties)
    val (_, create1) = create(builder, parties)
    val (cid2, create2) = create(builder, parties)
    val (_, create3) = create(builder, parties)
    builder.add(exercise(builder, create0, parties, true))
    builder.add(create1)
    builder.add(create2)
    val exeNid1 = builder.add(exercise(builder, create1, parties, false))
    val exeNid2 = builder.add(exercise(builder, create2, parties, true), exeNid1)
    builder.add(exercise(builder, create3, parties, false), exeNid2)
    val rollback = builder.add(builder.rollback(), exeNid2)
    builder.add(exercise(builder, create3, parties, true), rollback)
    val (cid4, create4) = create(builder, parties)
    builder.add(create4, rollback)
    val outerRollback = builder.add(builder.rollback())
    val innerRollback = builder.add(builder.rollback(), outerRollback)
    val (cid5, create5) = create(builder, parties)
    builder.add(create5, innerRollback)
    val transaction = builder.build()

    "consumedContracts does not include rollbacks" in {
      transaction.consumedContracts shouldBe Set(cid0, cid2)
    }
    "inactiveContracts includes rollbacks" in {
      transaction.inactiveContracts shouldBe Set(cid0, cid2, cid4, cid5)
    }
  }

  "updatedContractKeys" - {
    "return all the updated contract keys" in {
      val builder = new TxBuilder()
      val parties = Seq("Alice")
      val (cid0, create0) = create(builder, parties, Some("key0"))
      val (_, create1) = create(builder, parties, Some("key1"))
      val (_, create2) = create(builder, parties, Some("key2"))
      val (cid3, create3) = create(builder, parties, Some("key2"))
      val (_, create4) = create(builder, parties, Some("key2"))
      val (_, create5) = create(builder, parties, Some("key3"))
      val packageName = create0.packageName
      builder.add(create0)
      builder.add(exercise(builder, create0, parties, false))
      builder.add(create1)
      val ex = builder.add(exercise(builder, create1, parties, true))
      builder.add(create2, ex)
      builder.add(exercise(builder, create2, parties, true), ex)
      builder.add(create3, ex)
      val rollback = builder.add(builder.rollback())
      builder.add(exercise(builder, create0, parties, true), rollback)
      builder.add(create5, rollback)
      builder.add(exercise(builder, create3, parties, true), rollback)
      builder.add(create4, rollback)

      def key(s: String) =
        GlobalKey.assertBuild("Mod:T", V.ValueText(s), KeyPackageName(packageName, create0.version))
      builder.build().updatedContractKeys shouldBe
        Map(key("key0") -> Some(cid0), key("key1") -> None, key("key2") -> Some(cid3))
    }
  }

  "consumedBy" - {
    "non-consuming transaction with no rollbacks" - {
      "no nodes" in {
        val builder = new TxBuilder()
        val transaction = builder.build()

        transaction.consumedBy shouldBe Map.empty
      }

      "one node" - {
        "with local contracts" in {
          val builder = new TxBuilder()
          val parties = Seq("Alice")
          val (_, createNode0) = create(builder, parties, Some("key0"))

          builder.add(createNode0)
          val transaction = builder.build()

          transaction.consumedBy shouldBe Map.empty
        }

        "with global contracts" in {
          val builder = new TxBuilder()
          val parties = Seq("Alice")
          val (_, createNode0) = create(builder, parties, Some("key0"))
          val fetchNode0 = builder.fetch(createNode0, true)

          builder.add(fetchNode0)
          val transaction = builder.build()

          transaction.consumedBy shouldBe Map.empty
        }
      }

      "multiple nodes" - {
        "only create nodes" in {
          val builder = new TxBuilder()
          val parties = Seq("Alice")
          val (_, createNode0) = create(builder, parties, Some("key0"))
          val (_, createNode1) = create(builder, parties, Some("key1"))

          builder.add(createNode0)
          builder.add(createNode1)
          val transaction = builder.build()

          transaction.consumedBy shouldBe Map.empty
        }

        "create and non-consuming exercise nodes" - {
          "with local contracts" in {
            val builder = new TxBuilder()
            val parties = Seq("Alice")
            val (_, createNode0) = create(builder, parties, Some("key0"))

            builder.add(createNode0)
            builder.add(exercise(builder, createNode0, parties, false))
            val transaction = builder.build()

            transaction.consumedBy shouldBe Map.empty
          }

          "with global contracts" in {
            val builder = new TxBuilder()
            val parties = Seq("Alice")
            val (_, createNode0) = create(builder, parties, Some("key0"))

            builder.add(exercise(builder, createNode0, parties, false))
            val transaction = builder.build()

            transaction.consumedBy shouldBe Map.empty
          }
        }
      }
    }

    "consuming transaction with no rollbacks" - {
      "one exercise" - {
        "with local contracts" in {
          val builder = new TxBuilder()
          val parties = Seq("Alice")
          val (cid0, createNode0) = create(builder, parties, Some("key0"))

          builder.add(createNode0)
          val exerciseId0 = builder.add(exercise(builder, createNode0, parties, true))
          val transaction = builder.build()

          transaction.consumedBy shouldBe
            Map(cid0 -> exerciseId0)
        }

        "with global contracts" in {
          val builder = new TxBuilder()
          val parties = Seq("Alice")
          val (cid0, createNode0) = create(builder, parties, Some("key0"))

          val exerciseId0 = builder.add(exercise(builder, createNode0, parties, true))
          val transaction = builder.build()

          transaction.consumedBy shouldBe
            Map(cid0 -> exerciseId0)
        }
      }

      "multiple exercises" - {
        "with local contracts" in {
          val builder = new TxBuilder()
          val parties = Seq("Alice")
          val (cid0, createNode0) = create(builder, parties, Some("key0"))
          val (cid1, createNode1) = create(builder, parties, Some("key1"))

          builder.add(createNode0)
          builder.add(createNode1)
          val exerciseId0 = builder.add(exercise(builder, createNode0, parties, true))
          val exerciseId1 = builder.add(exercise(builder, createNode1, parties, true))
          val transaction = builder.build()

          transaction.consumedBy shouldBe
            Map(cid0 -> exerciseId0, cid1 -> exerciseId1)
        }

        "with global contracts" in {
          val builder = new TxBuilder()
          val parties = Seq("Alice")
          val (cid0, createNode0) = create(builder, parties, Some("key0"))
          val (cid1, createNode1) = create(builder, parties, Some("key1"))

          val exerciseId0 = builder.add(exercise(builder, createNode0, parties, true))
          val exerciseId1 = builder.add(exercise(builder, createNode1, parties, true))
          val transaction = builder.build()

          transaction.consumedBy shouldBe
            Map(cid0 -> exerciseId0, cid1 -> exerciseId1)
        }
      }
    }

    "consuming transaction with rollbacks" - {
      "one rollback" - {
        "with local contracts" in {
          val builder = new TxBuilder()
          val parties = Seq("Alice")
          val (cid0, createNode0) = create(builder, parties, Some("key0"))
          val (_, createNode1) = create(builder, parties, Some("key1"))

          builder.add(createNode0)
          builder.add(createNode1)
          val nodeId0 = builder.add(exercise(builder, createNode0, parties, true))
          val rollbackId = builder.add(builder.rollback())
          builder.add(exercise(builder, createNode1, parties, true), rollbackId)
          val transaction = builder.build()

          transaction.consumedBy shouldBe
            Map(cid0 -> nodeId0)
        }

        "with global contracts" in {
          val builder = new TxBuilder()
          val parties = Seq("Alice")
          val (cid0, createNode0) = create(builder, parties, Some("key0"))
          val (_, createNode1) = create(builder, parties, Some("key1"))

          val nodeId0 = builder.add(exercise(builder, createNode0, parties, true))
          val rollbackId = builder.add(builder.rollback())
          builder.add(exercise(builder, createNode1, parties, true), rollbackId)
          val transaction = builder.build()

          transaction.consumedBy shouldBe
            Map(cid0 -> nodeId0)
        }
      }

      "multiple rollbacks" - {
        "sequential rollbacks" - {
          "with local contracts" in {
            val builder = new TxBuilder()
            val parties = Seq("Alice")
            val (_, createNode0) = create(builder, parties, Some("key0"))
            val (_, createNode1) = create(builder, parties, Some("key1"))

            builder.add(createNode0)
            builder.add(createNode1)
            val rollbackId0 = builder.add(builder.rollback())
            builder.add(exercise(builder, createNode0, parties, true), rollbackId0)
            val rollbackId1 = builder.add(builder.rollback())
            builder.add(exercise(builder, createNode1, parties, true), rollbackId1)
            val transaction = builder.build()

            transaction.consumedBy shouldBe Map.empty
          }

          "with global contracts" in {
            val builder = new TxBuilder()
            val parties = Seq("Alice")
            val (_, createNode0) = create(builder, parties, Some("key0"))
            val (_, createNode1) = create(builder, parties, Some("key1"))

            val rollbackId0 = builder.add(builder.rollback())
            builder.add(exercise(builder, createNode0, parties, true), rollbackId0)
            val rollbackId1 = builder.add(builder.rollback())
            builder.add(exercise(builder, createNode1, parties, true), rollbackId1)
            val transaction = builder.build()

            transaction.consumedBy shouldBe Map.empty
          }
        }

        "nested rollbacks" - {
          "2 deep and 2 rollbacks" - {
            "with local contracts" in {
              val builder = new TxBuilder()
              val parties = Seq("Alice")
              val (_, createNode0) = create(builder, parties, Some("key0"))
              val (_, createNode1) = create(builder, parties, Some("key1"))

              builder.add(createNode0)
              builder.add(createNode1)
              val rollbackId0 = builder.add(builder.rollback())
              builder.add(exercise(builder, createNode0, parties, true), rollbackId0)
              val rollbackId1 = builder.add(builder.rollback(), rollbackId0)
              builder.add(exercise(builder, createNode1, parties, true), rollbackId1)
              val transaction = builder.build()

              transaction.consumedBy shouldBe Map.empty
            }

            "with global contracts" in {
              val builder = new TxBuilder()
              val parties = Seq("Alice")
              val (_, createNode0) = create(builder, parties, Some("key0"))
              val (_, createNode1) = create(builder, parties, Some("key1"))

              val rollbackId0 = builder.add(builder.rollback())
              builder.add(exercise(builder, createNode0, parties, true), rollbackId0)
              val rollbackId1 = builder.add(builder.rollback(), rollbackId0)
              builder.add(exercise(builder, createNode1, parties, true), rollbackId1)
              val transaction = builder.build()

              transaction.consumedBy shouldBe Map.empty
            }
          }

          "2 deep and 3 rollbacks" - {
            "with local contracts" in {
              val builder = new TxBuilder()
              val parties = Seq("Alice")
              val (_, createNode0) = create(builder, parties, Some("key0"))
              val (_, createNode1) = create(builder, parties, Some("key1"))
              val (_, createNode2) = create(builder, parties, Some("key2"))

              builder.add(createNode0)
              builder.add(createNode1)
              builder.add(createNode2)
              val rollbackId0 = builder.add(builder.rollback())
              builder.add(exercise(builder, createNode0, parties, true), rollbackId0)
              val rollbackId1 = builder.add(builder.rollback(), rollbackId0)
              builder.add(exercise(builder, createNode1, parties, true), rollbackId1)
              val rollbackId2 = builder.add(builder.rollback(), rollbackId0)
              builder.add(exercise(builder, createNode2, parties, true), rollbackId2)
              val transaction = builder.build()

              transaction.consumedBy shouldBe Map.empty
            }

            "with global contracts" in {
              val builder = new TxBuilder()
              val parties = Seq("Alice")
              val (_, createNode0) = create(builder, parties, Some("key0"))
              val (_, createNode1) = create(builder, parties, Some("key1"))
              val (_, createNode2) = create(builder, parties, Some("key2"))

              val rollbackId0 = builder.add(builder.rollback())
              builder.add(exercise(builder, createNode0, parties, true), rollbackId0)
              val rollbackId1 = builder.add(builder.rollback(), rollbackId0)
              builder.add(exercise(builder, createNode1, parties, true), rollbackId1)
              val rollbackId2 = builder.add(builder.rollback(), rollbackId0)
              builder.add(exercise(builder, createNode2, parties, true), rollbackId2)
              val transaction = builder.build()

              transaction.consumedBy shouldBe Map.empty
            }
          }

          "3 deep" - {
            "with local contracts" in {
              val builder = new TxBuilder()
              val parties = Seq("Alice")
              val (_, createNode0) = create(builder, parties, Some("key0"))
              val (_, createNode1) = create(builder, parties, Some("key1"))
              val (_, createNode2) = create(builder, parties, Some("key2"))

              builder.add(createNode0)
              builder.add(createNode1)
              builder.add(createNode2)
              val rollbackId0 = builder.add(builder.rollback())
              builder.add(exercise(builder, createNode0, parties, true), rollbackId0)
              val rollbackId1 = builder.add(builder.rollback(), rollbackId0)
              builder.add(exercise(builder, createNode1, parties, true), rollbackId1)
              val rollbackId2 = builder.add(builder.rollback(), rollbackId1)
              builder.add(exercise(builder, createNode2, parties, true), rollbackId2)
              val transaction = builder.build()

              transaction.consumedBy shouldBe Map.empty
            }

            "with global contracts" in {
              val builder = new TxBuilder()
              val parties = Seq("Alice")
              val (_, createNode0) = create(builder, parties, Some("key0"))
              val (_, createNode1) = create(builder, parties, Some("key1"))
              val (_, createNode2) = create(builder, parties, Some("key2"))

              val rollbackId0 = builder.add(builder.rollback())
              builder.add(exercise(builder, createNode0, parties, true), rollbackId0)
              val rollbackId1 = builder.add(builder.rollback(), rollbackId0)
              builder.add(exercise(builder, createNode1, parties, true), rollbackId1)
              val rollbackId2 = builder.add(builder.rollback(), rollbackId1)
              builder.add(exercise(builder, createNode2, parties, true), rollbackId2)
              val transaction = builder.build()

              transaction.consumedBy shouldBe Map.empty
            }
          }
        }
      }
    }
  }
}

object TransactionSpec {

  import TransactionBuilder.Implicits._

  class TxBuilder extends NodeIdTransactionBuilder with TestNodeBuilder

  def cid(s: String): V.ContractId = V.ContractId.V1(Hash.hashPrivateKey(s))

  def mkTransaction(
      nodes: HashMap[NodeId, Node],
      roots: ImmArray[NodeId],
  ): Transaction = Transaction(nodes, roots)

  def dummyRollbackNode(
      children: ImmArray[NodeId]
  ): Node.Rollback =
    Node.Rollback(
      children = children
    )

  def dummyExerciseNode(
      cid: V.ContractId,
      children: ImmArray[NodeId],
      hasExerciseResult: Boolean = true,
  ): Node.Exercise =
    Node.Exercise(
      targetCoid = cid,
      packageName = None,
      templateId = "DummyModule:dummyName",
      interfaceId = None,
      choiceId = "dummyChoice",
      consuming = true,
      actingParties = Set.empty,
      chosenValue = V.ValueUnit,
      stakeholders = Set.empty,
      signatories = Set.empty,
      choiceObservers = Set.empty,
      choiceAuthorizers = None,
      children = children,
      exerciseResult = if (hasExerciseResult) Some(V.ValueUnit) else None,
      keyOpt = None,
      byKey = false,
      version = TransactionVersion.minVersion,
    )

  def dummyCreateNode(createCid: V.ContractId): Node.Create =
    Node.Create(
      coid = createCid,
      packageNameVersion = None,
      templateId = Ref.Identifier.assertFromString("-dummyPkg-:DummyModule:dummyName"),
      arg = V.ValueContractId(cid("#dummyCid")),
      agreementText = "dummyAgreement",
      signatories = Set.empty,
      stakeholders = Set.empty,
      keyOpt = None,
      version = TransactionVersion.minVersion,
    )

}
