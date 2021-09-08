// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.{Bytes, ImmArray, Ref}
import com.daml.lf.transaction.GenTransaction.{
  AliasedNode,
  DanglingNodeId,
  NotWellFormedError,
  OrphanedNode,
}
import com.daml.lf.transaction.Node.{GenNode, NodeCreate, NodeExercises, NodeRollback}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.test.ValueGenerators.danglingRefGenNode
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec

import scala.collection.immutable.HashMap
import scala.language.implicitConversions
import scala.util.Random

class TransactionSpec
    extends AnyFreeSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {

  import TransactionSpec._

  "isWellFormed" - {
    "detects dangling references in roots" in {
      val tx = mkTransaction(HashMap.empty, ImmArray(NodeId(1)))
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(1), DanglingNodeId))
    }

    "detects dangling references in children" in {
      val tx = mkTransaction(
        HashMap(NodeId(1) -> dummyExerciseNode("cid1", ImmArray(NodeId(2)))),
        ImmArray(NodeId(1)),
      )
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(2), DanglingNodeId))
    }

    "detects cycles" in {
      val tx = mkTransaction(
        HashMap(NodeId(1) -> dummyExerciseNode("cid1", ImmArray(NodeId(1)))),
        ImmArray(NodeId(1)),
      )
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(1), AliasedNode))
    }

    "detects aliasing from roots and exercise" in {
      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyExerciseNode("cid0", ImmArray(NodeId(1))),
          NodeId(1) -> dummyExerciseNode("cid1", ImmArray(NodeId(2))),
          NodeId(2) -> dummyCreateNode("cid2"),
        ),
        ImmArray(NodeId(0), NodeId(2)),
      )
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(2), AliasedNode))
    }

    "detects orphans" in {
      val tx = mkTransaction(HashMap(NodeId(1) -> dummyCreateNode("cid1")), ImmArray.Empty)
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(1), OrphanedNode))
    }
  }

  "cids" - {

    "collects contract IDs" in {
      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyExerciseNode("cid0", ImmArray(NodeId(1))),
          NodeId(1) -> dummyExerciseNode("cid1", ImmArray(NodeId(2))),
          NodeId(2) -> dummyCreateNode("cid2"),
        ),
        ImmArray(NodeId(0), NodeId(2)),
      )

      def collectCids(tx: Transaction): Set[V.ContractId] = {
        val cids = Set.newBuilder[V.ContractId]
        tx.foreach2(_ => (), cids += _)
        cids.result()
      }

      collectCids(tx) shouldBe Set[V.ContractId]("cid0", "cid1", "cid2", dummyCid)

    }

  }

  "foldInExecutionOrder" - {
    "should traverse the transaction in execution order" in {

      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyCreateNode("cid0"),
          NodeId(1) -> dummyExerciseNode("cid1", ImmArray(NodeId(2), NodeId(4))),
          NodeId(2) -> dummyExerciseNode("cid2", ImmArray.Empty),
          NodeId(3) -> dummyCreateNode("cid3"),
          NodeId(4) -> dummyRollbackNode(ImmArray(NodeId(5))),
          NodeId(5) -> dummyCreateNode("cid5"),
        ),
        ImmArray(NodeId(0), NodeId(1), NodeId(3)),
      )

      val result = tx.foldInExecutionOrder(List.empty[String])(
        (acc, nid, _) => (s"exerciseBegin(${nid.index})" :: acc, true),
        (acc, nid, _) => (s"rollbackBegin(${nid.index})" :: acc, true),
        (acc, nid, _) => s"leaf(${nid.index})" :: acc,
        (acc, nid, _) => s"exerciseEnd(${nid.index})" :: acc,
        (acc, nid, _) => s"rollbackEnd(${nid.index})" :: acc,
      )

      result.reverse.mkString(", ") shouldBe
        "leaf(0), exerciseBegin(1), exerciseBegin(2), exerciseEnd(2), rollbackBegin(4), leaf(5), rollbackEnd(4), exerciseEnd(1), leaf(3)"
    }
  }

  "reachableNodeIds" - {
    "should collect the node-ids reachable from the roots" in {

      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyCreateNode("cid0"),
          NodeId(1) -> dummyExerciseNode("cid1", ImmArray(NodeId(2), NodeId(4))),
          NodeId(2) -> dummyExerciseNode("cid2", ImmArray.Empty),
          NodeId(3) -> dummyCreateNode("cid3"),
          NodeId(4) -> dummyRollbackNode(ImmArray(NodeId(5))),
          NodeId(5) -> dummyCreateNode("cid5"),
          // these are not reachable
          NodeId(10) -> dummyCreateNode("cid10"),
          NodeId(11) -> dummyExerciseNode("cid11", ImmArray(NodeId(12), NodeId(14))),
          NodeId(12) -> dummyExerciseNode("cid12", ImmArray.Empty),
          NodeId(13) -> dummyCreateNode("cid13"),
          NodeId(14) -> dummyRollbackNode(ImmArray(NodeId(15))),
          NodeId(15) -> dummyCreateNode("cid15"),
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
    def genTrans(node: GenNode[NodeId, ContractId]) = {
      val nid = NodeId(1)
      val version = node.optVersion.getOrElse(TransactionVersion.minExceptions)
      VersionedTransaction(version, HashMap(nid -> node), ImmArray(nid))
    }

    def isReplayedBy(
        n1: GenNode[NodeId, ContractId],
        n2: GenNode[NodeId, ContractId],
    ) = Validation.isReplayedBy(genTrans(n1), genTrans(n2))

    // the whole-transaction-relevant parts are handled by equalForest testing
    val genEmptyNode: Gen[GenNode[Nothing, V.ContractId]] =
      for {
        entry <- danglingRefGenNode
        node = entry match {
          case (_, nr: Node.NodeRollback[_]) =>
            nr.copy(children = ImmArray.Empty)
          case (_, n: Node.LeafOnlyActionNode[V.ContractId]) => n
          case (_, ne: Node.NodeExercises[_, V.ContractId]) =>
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
        val version = n.optVersion.getOrElse(TransactionVersion.minExceptions)
        n match {
          case _: NodeRollback[_] => ()
          case n: Node.GenActionNode[_, _] =>
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

      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyCreateNode("cid1"),
          NodeId(0) -> dummyExerciseNode("cid1", ImmArray(NodeId(0))),
          NodeId(1) -> dummyExerciseNode("cid2", ImmArray(NodeId(1))),
        ),
        ImmArray(NodeId(0), NodeId(1)),
      )

      val suffix1 = Bytes.assertFromString("01")
      val suffix2 = Bytes.assertFromString("02")

      val cid1 = toCid("cid1")
      val cid2 = toCid("cid2")

      val mapping1: crypto.Hash => Bytes = Map(
        cid1.discriminator -> suffix1,
        cid2.discriminator -> suffix2,
      )

      val mapping2: V.ContractId => V.ContractId = Map(
        cid1 -> V.ContractId.V1.assertBuild(cid1.discriminator, suffix1),
        cid2 -> V.ContractId.V1.assertBuild(cid2.discriminator, suffix2),
      )

      dummyCreateNode("dd").arg.suffixCid(mapping1)

      val tx1 = tx.suffixCid(mapping1)
      val tx2 = tx.suffixCid(mapping1)

      tx1 shouldNot be(tx)
      tx2 shouldBe tx1
      tx1 shouldBe Right(tx.map2(identity, mapping2))

    }
    "suffixing v0 contract id should be a no op" in {

      val v0Cid = V.ValueContractId(V.ContractId.V0.assertFromString("#deadbeef"))
      val Right(v0CidSuffixed) = v0Cid.suffixCid(_ => Bytes.assertFromString("cafe"))
      v0Cid shouldBe v0CidSuffixed

    }
  }

  "contractKeys" - {
    "return all the contract keys" in {
      val builder = TransactionBuilder(TransactionVersion.StableVersions.max)
      val parties = List("Alice")

      def create(s: String) = builder
        .create(
          id = s"#$s",
          template = s"-pkg-:Mod:$s",
          argument = V.ValueUnit,
          signatories = parties,
          observers = parties,
          key = Some(V.ValueText(s)),
        )

      def exe(s: String, consuming: Boolean, byKey: Boolean) =
        builder
          .exercise(
            contract = create(s),
            choice = s"Choice$s",
            actingParties = parties.toSet,
            consuming = consuming,
            argument = V.ValueUnit,
            byKey = byKey,
          )

      def fetch(s: String, byKey: Boolean) =
        builder.fetch(contract = create(s), byKey = byKey)

      def lookup(s: String, found: Boolean) =
        builder.lookupByKey(contract = create(s), found = found)

      val root1 =
        builder.create(
          "#root",
          template = "-pkg-:Mod:Root",
          argument = V.ValueUnit,
          signatories = parties,
          observers = parties,
          key = None,
        )

      val root2 = builder.exercise(
        root1,
        "ExerciseRoot",
        actingParties = parties.toSet,
        consuming = true,
        argument = V.ValueUnit,
      )

      builder.add(root1)
      val exeId = builder.add(root2)
      builder.add(create("Create"))
      builder.add(exe("NonConsumingExerciseById", false, false), exeId)
      builder.add(exe("ConsumingExerciseById", true, false), exeId)
      builder.add(exe("NonConsumingExerciseByKey", false, true), exeId)
      builder.add(exe("NonConsumingExerciseByKey", true, true), exeId)
      builder.add(fetch("FetchById", false), exeId)
      builder.add(fetch("FetchByKey", true), exeId)
      builder.add(lookup("SuccessfulLookup", true), exeId)
      builder.add(lookup("UnsuccessfulLookup", true), exeId)
      val rollbackId = builder.add(Node.NodeRollback(ImmArray.Empty))
      builder.add(create("RolledBackCreate"))
      builder.add(exe("RolledBackNonConsumingExerciseById", false, false), rollbackId)
      builder.add(exe("RolledBackConsumingExerciseById", true, false), rollbackId)
      builder.add(exe("RolledBackNonConsumingExerciseByKey", false, true), rollbackId)
      builder.add(exe("RolledBackNonConsumingExerciseByKey", true, true), rollbackId)
      builder.add(fetch("RolledBackFetchById", false), rollbackId)
      builder.add(fetch("RolledBackFetchByKey", true), rollbackId)
      builder.add(lookup("RolledBackSuccessfulLookup", true), rollbackId)
      builder.add(lookup("RolledBackUnsuccessfulLookup", true), rollbackId)

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
        ).map(s =>
          GlobalKey.assertBuild(Ref.Identifier.assertFromString(s"-pkg-:Mod:$s"), V.ValueText(s))
        ).toSet

      builder.build().contractKeys shouldBe expectedResults
    }
  }

  "contractKeyInputs" - {
    import Transaction._
    val dummyBuilder = TransactionBuilder(TransactionVersion.StableVersions.max)
    val parties = List("Alice")
    val tmplId = Ref.Identifier.assertFromString("-pkg-:Mod:T")
    def keyValue(s: String) = V.ValueText(s)
    def globalKey(s: String) = GlobalKey(tmplId, keyValue(s))
    def create(s: String) = dummyBuilder
      .create(
        id = s,
        template = "-pkg-:Mod:T",
        argument = V.ValueUnit,
        signatories = parties,
        observers = parties,
        key = Some(keyValue(s)),
      )

    def exe(s: String, consuming: Boolean, byKey: Boolean) =
      dummyBuilder
        .exercise(
          contract = create(s),
          choice = "Choice",
          actingParties = parties.toSet,
          consuming = consuming,
          argument = V.ValueUnit,
          byKey = byKey,
        )

    def fetch(s: String, byKey: Boolean) =
      dummyBuilder.fetch(contract = create(s), byKey = byKey)

    def lookup(s: String, found: Boolean) =
      dummyBuilder.lookupByKey(contract = create(s), found = found)

    "return None for create" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val createNode = create("#0")
      builder.add(createNode)
      builder.build().contractKeyInputs shouldBe Right(Map(globalKey("#0") -> KeyCreate))
    }
    "return Some(_) for fetch and fetch-by-key" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val fetchNode0 = fetch("#0", byKey = false)
      val fetchNode1 = fetch("#1", byKey = true)
      builder.add(fetchNode0)
      builder.add(fetchNode1)
      builder.build().contractKeyInputs shouldBe Right(
        Map(
          globalKey("#0") -> KeyActive(fetchNode0.coid),
          globalKey("#1") -> KeyActive(fetchNode1.coid),
        )
      )
    }
    "return Some(_) for consuming/non-consuming exercise and exercise-by-key" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val exe0 = exe("#0", consuming = false, byKey = false)
      val exe1 = exe("#1", consuming = true, byKey = false)
      val exe2 = exe("#2", consuming = false, byKey = true)
      val exe3 = exe("#3", consuming = true, byKey = true)
      builder.add(exe0)
      builder.add(exe1)
      builder.add(exe2)
      builder.add(exe3)
      builder.build().contractKeyInputs shouldBe
        Right(
          Seq(exe0, exe1, exe2, exe3).view
            .map(exe => globalKey(exe.targetCoid.coid) -> KeyActive(exe.targetCoid))
            .toMap
        )
    }

    "return None for negative lookup by key" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val lookupNode = lookup("#0", found = false)
      builder.add(lookupNode)
      builder.build().contractKeyInputs shouldBe Right(Map(globalKey("#0") -> NegativeKeyLookup))
    }

    "return Some(_) for negative lookup by key" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val lookupNode = lookup("#0", found = true)
      builder.add(lookupNode)
      inside(lookupNode.result) { case Some(cid) =>
        builder.build().contractKeyInputs shouldBe Right(Map(globalKey("#0") -> KeyActive(cid)))
      }
    }
    "returns keys used under rollback nodes" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val createNode = create("#0")
      val exerciseNode = exe("#1", consuming = false, byKey = false)
      val fetchNode = fetch("#2", byKey = false)
      val lookupNode = lookup("#3", found = false)
      val rollback = builder.add(builder.rollback())
      builder.add(createNode, rollback)
      builder.add(exerciseNode, rollback)
      builder.add(fetchNode, rollback)
      builder.add(lookupNode, rollback)
      builder.build().contractKeyInputs shouldBe Right(
        Map(
          globalKey("#0") -> KeyCreate,
          globalKey("#1") -> KeyActive(exerciseNode.targetCoid),
          globalKey("#2") -> KeyActive(fetchNode.coid),
          globalKey("#3") -> NegativeKeyLookup,
        )
      )
    }
    "two creates conflict" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      builder.add(create("#0"))
      builder.add(create("#0"))
      builder.build().contractKeyInputs shouldBe Left(DuplicateKeys(globalKey("#0")))
    }
    "two creates do not conflict if interleaved with archive" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      builder.add(create("#0"))
      builder.add(exe("#0", consuming = true, byKey = false))
      builder.add(create("#0"))
      builder.build().contractKeyInputs shouldBe Right(Map(globalKey("#0") -> KeyCreate))
    }
    "two creates do not conflict if one is in rollback" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val rollback = builder.add(builder.rollback())
      builder.add(create("#0"), rollback)
      builder.add(create("#0"))
      builder.build().contractKeyInputs shouldBe Right(Map(globalKey("#0") -> KeyCreate))
    }
    "negative lookup after create fails" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      builder.add(create("#0"))
      builder.add(lookup("#0", found = false))
      builder.build().contractKeyInputs shouldBe Left(InconsistentKeys(globalKey("#0")))
    }
    "inconsistent lookups conflict" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      builder.add(lookup("#0", found = true))
      builder.add(lookup("#0", found = false))
      builder.build().contractKeyInputs shouldBe Left(InconsistentKeys(globalKey("#0")))
    }
    "inconsistent lookups conflict across rollback" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val rollback = builder.add(builder.rollback())
      builder.add(lookup("#0", found = true), rollback)
      builder.add(lookup("#0", found = false))
      builder.build().contractKeyInputs shouldBe Left(InconsistentKeys(globalKey("#0")))
    }
    "positive lookup conflicts with create" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      builder.add(lookup("#0", found = true))
      builder.add(create("#0"))
      builder.build().contractKeyInputs shouldBe Left(DuplicateKeys(globalKey("#0")))
    }
    "positive lookup in rollback conflicts with create" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val rollback = builder.add(builder.rollback())
      builder.add(lookup("#0", found = true), rollback)
      builder.add(create("#0"))
      builder.build().contractKeyInputs shouldBe Left(DuplicateKeys(globalKey("#0")))
    }
    "rolled back archive does not prevent conflict" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      builder.add(create("#0"))
      val rollback = builder.add(builder.rollback())
      builder.add(exe("#0", consuming = true, byKey = true), rollback)
      builder.add(create("#0"))
      builder.build().contractKeyInputs shouldBe Left(DuplicateKeys(globalKey("#0")))
    }
    "successful, inconsistent lookups conflict" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val create0 = create("#0")
      val create1 = create("#1").copy(
        key = Some(
          Node.KeyWithMaintainers(
            key = keyValue("#0"),
            maintainers = Set.empty,
          )
        )
      )
      builder.add(builder.lookupByKey(create0, found = true))
      builder.add(builder.lookupByKey(create1, found = true))
      builder.build().contractKeyInputs shouldBe Left(InconsistentKeys(globalKey("#0")))
    }
    "first negative input wins" in {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val rollback = builder.add(builder.rollback())
      val create0 = create("#0")
      val lookup0 = builder.lookupByKey(create0, found = false)
      val create1 = create("#1")
      val lookup1 = builder.lookupByKey(create1, found = false)
      builder.add(create0, rollback)
      builder.add(lookup1, rollback)
      builder.add(lookup0)
      builder.add(create1)
      builder.build().contractKeyInputs shouldBe Right(
        Map(globalKey("#0") -> KeyCreate, globalKey("#1") -> NegativeKeyLookup)
      )
    }
  }

  def create(builder: TransactionBuilder, parties: Seq[String], key: Option[String] = None) = {
    val cid: ContractId = builder.newCid
    val node = builder.create(
      id = cid,
      template = "-pkg-:Mod:T",
      argument = V.ValueUnit,
      signatories = parties,
      observers = Seq(),
      key = key.map(V.ValueText(_)),
    )
    (cid, node)
  }
  def exercise(
      builder: TransactionBuilder,
      create: Node.NodeCreate[ContractId],
      parties: Seq[String],
      consuming: Boolean,
  ) =
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
    val builder = TransactionBuilder()
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
    val transaction = builder.build()

    "consumedContracs does not include rollbacks" in {
      transaction.consumedContracts shouldBe Set(cid0, cid2)
    }
    "inactiveContracts includes rollbacks" in {
      transaction.inactiveContracts shouldBe Set(cid0, cid2, cid4)
    }
  }

  "updatedContractKeys" - {
    "return all the updated contract keys" in {
      val builder = TransactionBuilder()
      val parties = Seq("Alice")
      val (cid0, create0) = create(builder, parties, Some("key0"))
      val (_, create1) = create(builder, parties, Some("key1"))
      val (_, create2) = create(builder, parties, Some("key2"))
      val (cid3, create3) = create(builder, parties, Some("key2"))
      val (_, create4) = create(builder, parties, Some("key2"))
      val (_, create5) = create(builder, parties, Some("key3"))
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
        GlobalKey.assertBuild(Ref.Identifier.assertFromString("-pkg-:Mod:T"), V.ValueText(s))
      builder.build().updatedContractKeys shouldBe
        Map(key("key0") -> Some(cid0), key("key1") -> None, key("key2") -> Some(cid3))
    }
  }
}

object TransactionSpec {
  type Transaction = GenTransaction[NodeId, V.ContractId]
  def mkTransaction(
      nodes: HashMap[NodeId, GenNode[NodeId, V.ContractId]],
      roots: ImmArray[NodeId],
  ): Transaction = GenTransaction(nodes, roots)

  def dummyRollbackNode(
      children: ImmArray[NodeId]
  ): NodeRollback[NodeId] =
    NodeRollback(
      children = children
    )

  def dummyExerciseNode(
      cid: V.ContractId,
      children: ImmArray[NodeId],
      hasExerciseResult: Boolean = true,
  ): NodeExercises[NodeId, V.ContractId] =
    NodeExercises(
      targetCoid = cid,
      templateId = Ref.Identifier(
        Ref.PackageId.assertFromString("-dummyPkg-"),
        Ref.QualifiedName.assertFromString("DummyModule:dummyName"),
      ),
      choiceId = "dummyChoice",
      consuming = true,
      actingParties = Set.empty,
      chosenValue = V.ValueUnit,
      stakeholders = Set.empty,
      signatories = Set.empty,
      choiceObservers = Set.empty,
      children = children,
      exerciseResult = if (hasExerciseResult) Some(V.ValueUnit) else None,
      key = None,
      byKey = false,
      version = TransactionVersion.minVersion,
    )

  val dummyCid = V.ContractId.V1.assertBuild(
    toCid("dummyCid").discriminator,
    Bytes.assertFromString("f00d"),
  )

  def dummyCreateNode(cid: String): NodeCreate[V.ContractId] =
    NodeCreate(
      coid = toCid(cid),
      templateId = Ref.Identifier.assertFromString("-dummyPkg-:DummyModule:dummyName"),
      arg = V.ValueContractId(dummyCid),
      agreementText = "dummyAgreement",
      signatories = Set.empty,
      stakeholders = Set.empty,
      key = None,
      version = TransactionVersion.minVersion,
    )

  implicit def toChoiceName(s: String): Ref.Name = Ref.Name.assertFromString(s)

  implicit def toCid(s: String): V.ContractId.V1 =
    V.ContractId.V1(crypto.Hash.hashPrivateKey(s))

}
