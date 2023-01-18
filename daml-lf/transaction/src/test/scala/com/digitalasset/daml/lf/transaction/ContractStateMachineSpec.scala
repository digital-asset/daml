// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package transaction

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.ContractStateMachine.{
  ActiveLedgerState,
  KeyActive,
  KeyInactive,
  KeyMapping,
  KeyResolver,
}
import com.daml.lf.transaction.ContractStateMachineSpec._
import com.daml.lf.transaction.Node.KeyWithMaintainers
import com.daml.lf.transaction.Transaction.{
  ChildrenRecursion,
  DuplicateContractKey,
  InconsistentContractKey,
  KeyCreate,
  KeyInput,
  KeyInputError,
  NegativeKeyLookup,
}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{
  defaultPackageId,
  toIdentifier,
  toName,
  toParty,
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

class ContractStateMachineSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  val alice: Ref.Party = "Alice"
  val aliceS: Set[Ref.Party] = Set(alice)
  val templateId: Ref.TypeConName = "Template:Id"
  val choiceId: Ref.ChoiceName = "Choice"
  val txVersion: TransactionVersion = TransactionVersion.maxVersion
  val unit: Value = Value.ValueUnit

  implicit def contractIdFromInt(coid: Int): ContractId = cid(coid)

  def cid(coid: Int): ContractId = {
    val bytes = Array.ofDim[Byte](crypto.Hash.underlyingHashLength)
    bytes(0) = (coid >> 24).toByte
    bytes(1) = (coid >> 16).toByte
    bytes(2) = (coid >> 8).toByte
    bytes(3) = coid.toByte
    val hash = crypto.Hash.assertFromByteArray(bytes)
    ContractId.V1(hash)
  }

  private def toKeyWithMaintainers(key: String): Option[KeyWithMaintainers] =
    if (key.isEmpty) None else Some(Node.KeyWithMaintainers(Value.ValueText(key), aliceS))

  def gkey(key: String): GlobalKey =
    GlobalKey.assertBuild(templateId, Value.ValueText(key))

  def mkCreate(
      contractId: ContractId,
      key: String = "",
  ): Node.Create =
    Node.Create(
      coid = contractId,
      templateId = templateId,
      arg = unit,
      agreementText = "",
      signatories = aliceS,
      stakeholders = aliceS,
      key = toKeyWithMaintainers(key),
      version = txVersion,
    )

  def mkExercise(
      contractId: ContractId,
      consuming: Boolean = true,
      key: String = "",
      byKey: Boolean = false,
  ): Node.Exercise = {
    Node.Exercise(
      targetCoid = contractId,
      templateId = templateId,
      interfaceId = None,
      choiceId = choiceId,
      consuming = consuming,
      actingParties = aliceS,
      chosenValue = unit,
      stakeholders = aliceS,
      signatories = aliceS,
      choiceObservers = Set.empty,
      children = ImmArray.Empty,
      exerciseResult = None,
      key = toKeyWithMaintainers(key),
      byKey = byKey,
      version = txVersion,
    )
  }

  def mkFetch(
      contractId: ContractId,
      key: String = "",
      byKey: Boolean = false,
  ): Node.Fetch = {
    Node.Fetch(
      coid = contractId,
      templateId = templateId,
      actingParties = aliceS,
      signatories = aliceS,
      stakeholders = aliceS,
      key = toKeyWithMaintainers(key),
      byKey = byKey,
      version = txVersion,
    )
  }

  def mkLookupByKey(
      key: String,
      contractId: Option[ContractId],
  ): Node.LookupByKey =
    Node.LookupByKey(
      templateId = templateId,
      key = Node.KeyWithMaintainers(Value.ValueText(key), aliceS),
      result = contractId,
      version = txVersion,
    )

  def inconsistentContractKey[X](key: GlobalKey): Left[KeyInputError, X] =
    Left(Left(InconsistentContractKey(key)))

  def duplicateContractKey[X](key: GlobalKey): Left[KeyInputError, X] =
    Left(Right(DuplicateContractKey(key)))

  def createRbExLbkLbk: TestCase = {
    // [ Create c1 (key=k1), Rollback [ Exe c1 [ LBK k1 -> None ]], LBK k1 -> c1 ]
    val builder = TransactionBuilder()
    val _ = builder.add(mkCreate(1, "key1"))
    val rollbackNid = builder.add(builder.rollback())
    val exerciseNid =
      builder.add(mkExercise(1, consuming = true, "key1", byKey = false), rollbackNid)
    val _ = builder.add(mkLookupByKey("key1", None), exerciseNid)
    val _ = builder.add(mkLookupByKey("key1", Some(1)))
    val tx = builder.build()
    val expected = Right(
      Map(gkey("key1") -> KeyCreate) ->
        ActiveLedgerState(Set(1), Map.empty, Map(gkey("key1") -> 1))
    )
    TestCase(
      "Create|Rb-Ex-LBK|LBK",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> expected,
        ContractKeyUniquenessMode.Off -> expected,
      ),
    )
  }

  def multipleRollback: TestCase = {
    // [ Exe c0 [ Rollback [ Exe c1 (key=k1, byKey), Create c2 (key=k1) ],
    //         Exe c1 (key=k, byKey) [ Rollback [ Create c3 (key=k1), ExeN c3 (key=k1, byKey) ] ],
    //         LBK k1 -> None ]
    val builder = TransactionBuilder()
    val exercise0Nid = builder.add(mkExercise(0))
    val rollback1Nid = builder.add(builder.rollback(), exercise0Nid)
    val _ = builder.add(mkExercise(1, consuming = true, "key1", byKey = true), rollback1Nid)
    val _ = builder.add(mkCreate(2, "key1"), rollback1Nid)
    val exercise1Nid =
      builder.add(mkExercise(1, consuming = true, "key1", byKey = true), exercise0Nid)
    val rollback2Nid = builder.add(builder.rollback(), exercise1Nid)
    val _ = builder.add(mkCreate(3, "key1"), rollback2Nid)
    val _ = builder.add(mkExercise(3, consuming = false, "key1", byKey = true), rollback2Nid)
    val _ = builder.add(mkLookupByKey("key1", None), exercise0Nid)
    val tx = builder.build()
    val expected = Right(
      Map(gkey("key1") -> Transaction.KeyActive(cid(1))) ->
        ActiveLedgerState(
          Set.empty,
          Map(cid(0) -> (), cid(1) -> ()),
          Map.empty,
        )
    )
    TestCase(
      "multiple rollback",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> expected,
        ContractKeyUniquenessMode.Off -> expected,
      ),
    )
  }

  def nestedRollback: TestCase = {
    // [ Rollback [ Fetch c1 (key=k1, byKey),
    //              Exe c2 (key=k2, !byKey),
    //              Rollback [ LBK k1 -> c1, LBK k2 -> None ],
    //              LBK k2 -> None,
    //              Exe c1 (key=k1, byKey) ] ]
    val builder = TransactionBuilder()
    val rollback1Nid = builder.add(builder.rollback())
    val _ = builder.add(mkFetch(1, "key1", byKey = true), rollback1Nid)
    val _ = builder.add(mkExercise(2, consuming = true, "key2"), rollback1Nid)
    val rollback2Nid = builder.add(builder.rollback(), rollback1Nid)
    val _ = builder.add(mkLookupByKey("key1", Some(cid(1))), rollback2Nid)
    val _ = builder.add(mkLookupByKey("key2", None), rollback2Nid)
    val _ = builder.add(mkLookupByKey("key2", None), rollback1Nid)
    val _ = builder.add(mkExercise(1, consuming = true, "key1", byKey = true))
    val tx = builder.build()
    val expected = Right(
      Map(gkey("key1") -> Transaction.KeyActive(1), gkey("key2") -> Transaction.KeyActive(2)) ->
        ActiveLedgerState(Set.empty, Map(cid(1) -> ()), Map.empty)
    )
    TestCase(
      "nested rollback",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> expected,
        ContractKeyUniquenessMode.Off -> expected,
      ),
    )
  }

  // Regression test for https://github.com/digital-asset/daml/pull/14080
  def archiveRbLookupCreate: TestCase = {
    // Exe c1
    //   [ Exe c2 (key=k, !byKey)
    //   , Rollback [ LBK k -> None ],
    //   , Create c3 (key=k)
    //   ]
    val builder = TransactionBuilder()
    val exerciseNid = builder.add(mkExercise(1))
    builder.add(mkExercise(1))
    builder.add(mkExercise(2, consuming = true, "key", byKey = false), exerciseNid)
    val rollbackNid = builder.add(builder.rollback(), exerciseNid)
    builder.add(mkLookupByKey("key", None), rollbackNid)
    builder.add(mkCreate(3, "key"), exerciseNid)
    val tx = builder.build()
    val expected: TestResult = Right(
      Map(gkey("key") -> Transaction.KeyActive(2)) -> ActiveLedgerState(
        Set(3),
        Map(cid(1) -> (), cid(2) -> ()),
        Map(gkey("key") -> cid(3)),
      )
    )
    TestCase(
      "ArchiveRbLookupCreate",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> expected,
        ContractKeyUniquenessMode.Off -> expected,
      ),
    )
  }

  def rbExeCreateLbkDivulged: TestCase = {
    // [ Exe c1 [ Rollback [ Exe c2 (key=k1, !byKey), Create c3 (key=k1) ], LBK k1 -> None ] ]
    // (c2 is divulged)
    val builder = TransactionBuilder()
    val exercise1Nid = builder.add(mkExercise(1))
    val rollbackNid = builder.add(builder.rollback(), exercise1Nid)
    val _ = builder.add(mkExercise(2, consuming = true, "key1"), rollbackNid)
    val _ = builder.add(mkCreate(3, "key1"), rollbackNid)
    val _ = builder.add(mkLookupByKey("key1", None), exercise1Nid)
    val tx = builder.build()
    // Custom resolver for visibility restriction due to divulgence
    val resolver = Map(gkey("key1") -> None)
    val expected = Right(
      Map(gkey("key1") -> KeyCreate) ->
        ActiveLedgerState(Set.empty, Map(cid(1) -> ()), Map.empty)
    )
    TestCase(
      "RbExeCreateLbkDivulged",
      tx,
      resolver,
      Map(
        ContractKeyUniquenessMode.Strict -> inconsistentContractKey(gkey("key1")),
        ContractKeyUniquenessMode.Off -> expected,
      ),
    )
  }

  def rbExeCreateFbk: TestCase = {
    // [ Exe c1 [ Rollback [ Exe c2 (key=k1, !byKey), Create c3 (key=k1) ], FetchByKey k1 -> c2 ] ]
    val builder = TransactionBuilder()
    val exercise1Nid = builder.add(mkExercise(1))
    val rollbackNid = builder.add(builder.rollback(), exercise1Nid)
    val _ = builder.add(mkExercise(2, consuming = true, "key1"), rollbackNid)
    val _ = builder.add(mkCreate(3, "key1"), rollbackNid)
    val _ = builder.add(mkFetch(2, "key1", byKey = true), exercise1Nid)
    val tx = builder.build()
    val expected = Right(
      Map(gkey("key1") -> Transaction.KeyActive(2)) ->
        ActiveLedgerState(Set.empty, Map(cid(1) -> ()), Map.empty)
    )
    TestCase(
      "RbExeCreateFbk",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> expected,
        ContractKeyUniquenessMode.Off -> // TODO This is a bug in the contract key logic
          inconsistentContractKey(gkey("key1")),
      ),
    )

  }

  def doubleCreate: TestCase = {
    // [ ExeN c1 [ Create c2 (key=k1), Create c3 (key=k1) ] ]
    val builder = TransactionBuilder()
    val exerciseNid = builder.add(mkExercise(1, consuming = false))
    val _ = builder.add(mkCreate(2, "key1"), exerciseNid)
    val _ = builder.add(mkCreate(3, "key1"), exerciseNid)
    val tx = builder.build()
    val expectedOff = Right(
      Map(gkey("key1") -> KeyCreate) ->
        ActiveLedgerState(
          Set(2, 3),
          Map.empty,
          Map(
            gkey("key1") -> cid(3) // Latest create wins
          ),
        )
    )
    TestCase(
      "DoubleCreate",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> duplicateContractKey(gkey("key1")),
        ContractKeyUniquenessMode.Off -> expectedOff,
      ),
    )
  }

  def divulgedLookup: TestCase = {
    // Key lookups don't find divulged contracts even though they can be used normally with exercise.
    // [ ExeN c1 (key=k1, !byKey) [ LBK k1 -> None ] ]
    val builder = TransactionBuilder()
    val exerciseNid = builder.add(mkExercise(1, consuming = false, "key1"))
    val _ = builder.add(mkLookupByKey("key1", None), exerciseNid)
    val tx = builder.build()
    val expected = Right(
      Map(gkey("key1") -> NegativeKeyLookup) ->
        ActiveLedgerState(Set.empty, Map.empty, Map.empty)
    )
    TestCase(
      "DivulgedLookup",
      tx,
      Map(gkey("key1") -> KeyInactive),
      Map(
        ContractKeyUniquenessMode.Strict -> inconsistentContractKey(gkey("key1")),
        ContractKeyUniquenessMode.Off -> expected,
      ),
    )
  }

  def rbFbkFetch: TestCase = {
    // Fetch-by-key a contract under a rollback
    // [ Exe c1 [ Rollback [ FBK k1 -> c2 ], Fetch c3 (key=k1) ]
    val builder = TransactionBuilder()
    val exerciseNid = builder.add(mkExercise(1))
    val rollbackNid = builder.add(builder.rollback(), exerciseNid)
    val _ = builder.add(mkFetch(2, "key1", byKey = true), rollbackNid)
    val _ = builder.add(mkFetch(3, "key1"), exerciseNid)
    val tx = builder.build()
    val expected = Right(
      Map(gkey("key1") -> Transaction.KeyActive(2)) ->
        ActiveLedgerState(Set.empty, Map(cid(1) -> ()), Map.empty)
    )
    TestCase(
      "FetchByKey-then-Fetch",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> inconsistentContractKey(gkey("key1")),
        ContractKeyUniquenessMode.Off -> expected,
      ),
    )
  }

  def archiveOtherKeyContract: TestCase = {
    // multiple keys
    // [ ExeN c1 [ FBK k1 -> c2, Exe c3 (key=k1, !byKey) [ LBK k1 -> c2 ] ]
    val builder = TransactionBuilder()
    val exerciseNid = builder.add(mkExercise(1, consuming = false))
    val _ = builder.add(mkFetch(2, "key1", byKey = true), exerciseNid)
    val exercise2Nid = builder.add(mkExercise(3, consuming = true, "key1"), exerciseNid)
    val _ = builder.add(mkLookupByKey("key1", Some(2)), exercise2Nid)
    val tx = builder.build()
    val expected = Right(
      Map(gkey("key1") -> Transaction.KeyActive(2)) ->
        ActiveLedgerState(Set.empty, Map(cid(3) -> ()), Map.empty)
    )
    TestCase(
      "Archive other contract with key",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> inconsistentContractKey(gkey("key1")),
        ContractKeyUniquenessMode.Off -> expected,
      ),
    )
  }

  def createAfterRbArchive: TestCase = {
    // [ Rollback [ Exe c1 (key=k1, !byKey), Create c2 (key=k1) ], Create c3 (key=k1) ]
    val builder = TransactionBuilder()
    val rollbackNid = builder.add(builder.rollback())
    val _ = builder.add(mkExercise(1, consuming = true, "key1"), rollbackNid)
    val _ = builder.add(mkCreate(2, "key1"), rollbackNid)
    val _ = builder.add(mkCreate(3, "key1"))
    val tx = builder.build()
    val expected = Right(
      Map(gkey("key1") -> KeyCreate) ->
        ActiveLedgerState(Set(3), Map.empty, Map(gkey("key1") -> cid(3)))
    )
    TestCase(
      "CreateAfterRbExercise",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> duplicateContractKey(gkey("key1")),
        ContractKeyUniquenessMode.Off -> expected,
      ),
    )
  }

  def differingCause1: TestCase = {
    // [ Create c1 (key = k1), ExeN c2 [ Create c3 (key = k1), LookupByKey k1 -> None ] ]
    // In ContractKeyUniquenessMode.Strict,
    // iterating over the ExeN subtree from an empty state fails with InconsistentKeys
    // but iterating over the whole transaction fails with DuplicateContractKey
    val builder = TransactionBuilder()
    val _ = builder.add(mkCreate(1, "key1"))
    val exerciseNid = builder.add(mkExercise(2, consuming = false))
    val _ = builder.add(mkCreate(3, "key1"), exerciseNid)
    val _ = builder.add(mkLookupByKey("key1", None), exerciseNid)
    val tx = builder.build()
    val expected = duplicateContractKey(gkey("key1"))
    TestCase(
      "differing cause 1",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> expected,
        ContractKeyUniquenessMode.Off -> inconsistentContractKey(gkey("key1")),
      ),
    )
  }

  def differingCause2: TestCase = {
    // [ Create c1 (key = k1), ExeN c2 [ Create c3 (key = k2), Create c4 (key=k1), Create c5 (key = k2) ]
    // In ContractKeyUniquenessMode.Strict,
    // iterating over the ExeN subtree from an empty state fails with DuplicateContractKeys(k2)
    // while iterating over the whole transaction fails with DuplicateContractKeys(k1)
    val builder = TransactionBuilder()
    val _ = builder.add(mkCreate(1, "key1"))
    val exerciseNid = builder.add(mkExercise(2, consuming = false))
    val _ = builder.add(mkCreate(3, "key2"), exerciseNid)
    val _ = builder.add(mkCreate(4, "key1"), exerciseNid)
    val _ = builder.add(mkCreate(5, "key2"), exerciseNid)
    val tx = builder.build()
    val expectedOff = Right(
      Map(gkey("key1") -> KeyCreate, gkey("key2") -> KeyCreate) ->
        ActiveLedgerState(
          Set(1, 3, 4, 5),
          Map.empty,
          Map(gkey("key1") -> cid(4), gkey("key2") -> cid(5)),
        )
    )
    TestCase(
      "differing cause 2",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> duplicateContractKey(gkey("key1")),
        ContractKeyUniquenessMode.Off -> expectedOff,
      ),
    )
  }

  def inconsistentFetchByKey: TestCase = {
    // Inconsistent fetch-by-key nodes separated by a Rollback
    // [ Exe c1 [ Rollback [ FBK k1 -> c2 ], FBK k1 -> c3 ]
    val builder = TransactionBuilder()
    val exerciseNid = builder.add(mkExercise(1))
    val rollbackNid = builder.add(builder.rollback(), exerciseNid)
    val _ = builder.add(mkFetch(2, "key1", byKey = true), rollbackNid)
    val _ = builder.add(mkFetch(3, "key1", byKey = true), exerciseNid)
    val tx = builder.build()
    val expected = inconsistentContractKey(gkey("key1"))
    TestCase(
      "inconsistent fetch-by-key",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> expected,
        ContractKeyUniquenessMode.Off -> expected,
      ),
    )
  }

  def rbCreate: TestCase = {
    // Exe c0 [ Rollback [ Create c1 ] ]
    val builder = TransactionBuilder()
    val exTop = builder.add(mkExercise(0))
    val rollbackNid = builder.add(builder.rollback(), exTop)
    builder.add(mkCreate(1), rollbackNid)
    val tx = builder.build()
    val expected = Right(
      Map[GlobalKey, KeyInput]() ->
        ActiveLedgerState(Set.empty, Map(cid(0) -> ()), Map.empty)
    )
    TestCase(
      "rbCreate",
      tx,
      Map(
        ContractKeyUniquenessMode.Strict -> expected,
        ContractKeyUniquenessMode.Off -> expected,
      ),
    )
  }

  // Note that we provide no stability for `ContractKeyUniquenessMode.Off`
  // or for transactions with multiple keys.
  // So these tests serve only as an indication of the current behavior
  // but can be changed freely.
  val testCases: Seq[TestCase] = Seq(
    createRbExLbkLbk,
    multipleRollback,
    nestedRollback,
    archiveRbLookupCreate,
    rbExeCreateLbkDivulged,
    rbExeCreateFbk,
    doubleCreate,
    divulgedLookup,
    rbFbkFetch,
    archiveOtherKeyContract,
    createAfterRbArchive,
    differingCause1,
    differingCause2,
    inconsistentFetchByKey,
    rbCreate,
  )

  "advance" should {

    testCases.foreach { case TestCase(name, tx, resolver, expected) =>
      s"pass $name" when {
        expected.foreach { case (mode, expectedResult) =>
          s"mode $mode" in {
            // We use `Unit` instead of `NodeId` so that we don't have to fiddle with node ids
            val ksm = new ContractStateMachine[Unit](mode)
            val actualResolver: KeyResolver =
              if (mode == ContractKeyUniquenessMode.Strict) Map.empty else resolver
            val result = visitSubtrees(ksm)(tx.nodes, tx.roots.toSeq, actualResolver, ksm.initial)

            (result, expectedResult) match {
              case (Left(err1), Left(err2)) => err1 shouldBe err2
              case (Right(state), Right((gkI, activeState))) =>
                withClue("global key inputs") {
                  state.globalKeyInputs shouldBe gkI
                }
                withClue("active state") {
                  state.activeState shouldBe activeState
                }
                state.rollbackStack shouldBe List.empty
              case _ => fail(s"$result was not equal to $expectedResult")
            }
          }
        }
      }
    }
  }

  "ActiveLedgerState.isEquivalent" should {
    val s = ActiveLedgerState(
      Set(1, 2, 3, 4, 5),
      Map[ContractId, Unit]((2, ()), (5, ())),
      Map(gkey("key1") -> 2, gkey("key2") -> 4),
    )
    "succeed on identical states" in {
      assert(s.isEquivalent(s))
    }
    "succeed if localKeys differ but localActiveKeys is identical" in {
      // Different entry that is also not active.
      val tweakedS = ActiveLedgerState(
        Set(1, 2, 3, 4, 5),
        Map[ContractId, Unit]((2, ()), (5, ())),
        Map(gkey("key1") -> 5, gkey("key2") -> 4),
      )
      assert(s.isEquivalent(tweakedS))
    }
    "fail if locallyCreatedThisTimeline is different" in {
      val tweakedS = s.copy(locallyCreatedThisTimeline = Set(1, 2, 3, 4))
      assert(!s.isEquivalent(tweakedS))
    }
    "fail if consumedBy is different" in {
      val tweakedS = s.copy(consumedBy = Map[ContractId, Unit]((2, ())))
      assert(!s.isEquivalent(tweakedS))
    }
    "fail it localActiveKeys is different" in {
      // No entry
      var tweakedS = ActiveLedgerState(
        Set(1, 2, 3, 4, 5),
        Map[ContractId, Unit]((2, ()), (5, ())),
        Map(gkey("key2") -> 4),
      )
      assert(!s.isEquivalent(tweakedS))
      // Different entry that is still active
      tweakedS = ActiveLedgerState(
        Set(1, 2, 3, 4, 5),
        Map[ContractId, Unit]((2, ()), (5, ())),
        Map(gkey("key1") -> 3, gkey("key2") -> 4),
      )
      assert(!s.isEquivalent(tweakedS))
    }
  }

  private def children(node: Node): ImmArray[NodeId] = node match {
    case _: Node.Create | _: Node.Fetch | _: Node.LookupByKey => ImmArray.empty[NodeId]
    case exercise: Node.Exercise => exercise.children
    case rollback: Node.Rollback => rollback.children
  }

  /** Visits the `root` node and all its children in execution order and updates the `state` accordingly,
    * using the following methods on [[com.daml.lf.transaction.ContractStateMachine.State]]:
    * - [[com.daml.lf.transaction.Node.Create]] calls [[com.daml.lf.transaction.ContractStateMachine.State.visitCreate]]
    * - [[com.daml.lf.transaction.Node.Fetch]] calls [[com.daml.lf.transaction.ContractStateMachine.State.handleFetch]]
    * - [[com.daml.lf.transaction.Node.Exercise]] calls [[com.daml.lf.transaction.ContractStateMachine.State.handleExercise]]
    *   before visiting the children
    * - [[com.daml.lf.transaction.Node.LookupByKey]] calls [[com.daml.lf.transaction.ContractStateMachine.State.handleLookup]]
    *   in mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]] and
    *   [[com.daml.lf.transaction.ContractStateMachine.State.handleLookupWith]]
    *   in modes [[com.daml.lf.transaction.ContractKeyUniquenessMode.ContractByKeyUniquenessMode]] using the `resolver`.
    * - [[com.daml.lf.transaction.Node.Rollback]] calls [[com.daml.lf.transaction.ContractStateMachine.State.beginRollback]]
    *   before visiting the children and
    *   [[com.daml.lf.transaction.ContractStateMachine.State.endRollback]] after visiting the children.
    *
    * @param resolver The resolver used in modes [[com.daml.lf.transaction.ContractKeyUniquenessMode.ContractByKeyUniquenessMode]]
    *                 for handling [[com.daml.lf.transaction.Node.LookupByKey]].
    *                 Ignored in mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]].
    */
  private def visitSubtree(ksm: ContractStateMachine[Unit])(
      nodes: Map[NodeId, Node],
      root: NodeId,
      resolver: KeyResolver,
      state: ksm.State,
  ): Either[Transaction.KeyInputError, ksm.State] = {
    val node = nodes(root)
    for {
      next <- node match {
        case actionNode: Node.Action =>
          lazy val gkeyO =
            actionNode.keyOpt.map(key => GlobalKey.assertBuild(actionNode.templateId, key.key))
          state.handleNode((), actionNode, gkeyO, resolver)
        case _: Node.Rollback =>
          Right(state.beginRollback())
      }
      afterChildren <- withClue(s"visiting children of $node") {
        visitSubtrees(ksm)(nodes, children(node).toSeq, resolver, next)
      }
      exited = node match {
        case _: Node.Rollback => afterChildren.endRollback()
        case _ => afterChildren
      }
    } yield exited
  }

  /** Fully visits the trees rooted at `roots` in execution order.
    * For each subtree visited, additionally visit this subtree starting from the initial state
    * and check that advancing the current state yields the same resulting state
    *
    * @see visitSubtree for how visiting nodes updates the state
    */
  private def visitSubtrees(ksm: ContractStateMachine[Unit])(
      nodes: Map[NodeId, Node],
      roots: Seq[NodeId],
      resolver: KeyResolver,
      state: ksm.State,
  ): Either[Transaction.KeyInputError, ksm.State] = {
    roots match {
      case Seq() => Right(state)
      case root +: tail =>
        val node = nodes(root)
        val directVisit = visitSubtree(ksm)(nodes, root, resolver, state)
        // Now project the resolver and visit the subtree from a fresh state and check whether we end up the same using advance
        val fresh = ksm.initial
        val projectedResolver: KeyResolver =
          if (state.mode == ContractKeyUniquenessMode.Strict) Map.empty
          else state.projectKeyResolver(resolver)
        withClue(
          s"Advancing over subtree rooted at $node with projected resolver $projectedResolver; projection state=$state; original resolver=$resolver"
        ) {
          val freshVisit = visitSubtree(ksm)(nodes, root, projectedResolver, fresh)
          val advanced = freshVisit.flatMap(substate => state.advance(resolver, substate))

          (directVisit, advanced) match {
            case (Right(direct), Right(adv)) =>
              withClue(s"visiting and advancing $node resulted in different states ") {
                direct shouldBe adv
              }
            case (Left(_), Left(_)) =>
            // We can't really make sure that we get the same errors.
            // There may be multiple key conflicts and advancing non-deterministically picks one of them
            case _ => fail(s"$directVisit was knot equal to $advanced")
          }
        }
        directVisit.flatMap(next => visitSubtrees(ksm)(nodes, tail, resolver, next))
    }
  }
}

object ContractStateMachineSpec {
  type TestResult = Either[KeyInputError, (Map[GlobalKey, KeyInput], ActiveLedgerState[Unit])]
  case class TestCase(
      name: String,
      transaction: HasTxNodes,
      resolver: KeyResolver,
      expected: Map[ContractKeyUniquenessMode, TestResult],
  )

  object TestCase {
    def apply(
        name: String,
        transaction: HasTxNodes,
        expected: Map[ContractKeyUniquenessMode, TestResult],
    ): TestCase = TestCase(name, transaction, resolverFromTx(transaction), expected)
  }

  def resolverFromTx(tx: HasTxNodes): KeyResolver = {
    def updateKey(
        resolver: KeyResolver,
        templateId: Ref.TypeConName,
        mbKey: Option[KeyWithMaintainers],
        mapping: KeyMapping,
    ): KeyResolver = mbKey.fold(resolver) { key =>
      val gkey = GlobalKey.assertBuild(templateId, key.key)
      if (resolver.contains(gkey)) resolver else resolver.updated(gkey, mapping)
    }

    tx.foldInExecutionOrder(Map.empty: KeyResolver)(
      exerciseBegin = (s, _, ex) =>
        updateKey(s, ex.templateId, ex.key, KeyActive(ex.targetCoid)) ->
          ChildrenRecursion.DoRecurse,
      exerciseEnd = (s, _, _) => s,
      leaf = (s, _, leaf) =>
        leaf match {
          case create: Node.Create => updateKey(s, create.templateId, create.key, KeyInactive)
          case fetch: Node.Fetch => updateKey(s, fetch.templateId, fetch.key, KeyActive(fetch.coid))
          case lookup: Node.LookupByKey =>
            updateKey(s, lookup.templateId, Some(lookup.key), lookup.result)
        },
      rollbackBegin = (s, _, _) => s -> ChildrenRecursion.DoRecurse,
      rollbackEnd = (s, _, _) => s,
    )
  }

}
