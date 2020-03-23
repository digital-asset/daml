// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import java.io.File
import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1.{Configuration, Offset, TimeModel}
import com.digitalasset.daml.bazeltools.BazelRunfiles.rlocation
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref.{Identifier, Party}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.{GenTransaction, Node}
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractInst,
  NodeId,
  ValueRecord,
  ValueText,
  ValueUnit,
  VersionedValue
}
import com.digitalasset.daml.lf.value.ValueVersions
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.ledger.{EventId, TransactionId}
import com.digitalasset.platform.events.EventIdFormatter
import com.digitalasset.platform.store.PersistenceEntry
import com.digitalasset.platform.store.entries.LedgerEntry
import org.scalatest.Suite

import scala.collection.immutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Success, Try}

private[dao] trait JdbcLedgerDaoSuite extends AkkaBeforeAndAfterAll with JdbcLedgerDaoBackend {
  this: Suite =>

  protected final val nextOffset: () => Offset = {
    val base = BigInt(1) << 32
    val counter = new AtomicLong(0)
    () =>
      Offset.fromByteArray((base + counter.getAndIncrement()).toByteArray)
  }

  protected final implicit class OffsetToLong(offset: Offset) {
    def toLong: Long = BigInt(offset.toByteArray).toLong
  }

  protected final val alice = Party.assertFromString("Alice")
  protected final val bob = Party.assertFromString("Bob")
  protected final val charlie = Party.assertFromString("Charlie")

  protected final val someAgreement = "agreement"
  protected final val someTemplateId = Identifier(
    Ref.PackageId.assertFromString("packageId"),
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString("moduleName"),
      Ref.DottedName.assertFromString("someTemplate"))
  )
  protected final val someRecordId = Identifier(
    Ref.PackageId.assertFromString("packageId"),
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString("moduleName"),
      Ref.DottedName.assertFromString("someRecord"))
  )
  protected final val someValueText = ValueText("some text")
  protected final val someValueRecord = ValueRecord(
    Some(someRecordId),
    ImmArray(Some(Ref.Name.assertFromString("field")) -> someValueText))
  protected final val someContractKey =
    VersionedValue(ValueVersions.acceptedVersions.head, someValueText)
  protected final val someContractInstance = ContractInst(
    someTemplateId,
    VersionedValue(ValueVersions.acceptedVersions.head, someValueRecord),
    someAgreement
  )

  protected final val defaultConfig = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault,
    Duration.ofDays(1),
  )

  private val reader = DarReader { (_, stream) =>
    Try(DamlLf.Archive.parseFrom(stream))
  }
  private val Success(dar) =
    reader.readArchiveFromFile(new File(rlocation("ledger/test-common/Test-stable.dar")))
  private val now = Instant.now()

  protected final val packages: List[(DamlLf.Archive, v2.PackageDetails)] =
    dar.all.map(dar => dar -> v2.PackageDetails(dar.getSerializedSize.toLong, now, None))

  protected implicit def toParty(s: String): Ref.Party = Ref.Party.assertFromString(s)

  protected implicit def toLedgerString(s: String): Ref.LedgerString =
    Ref.LedgerString.assertFromString(s)

  protected final def event(txid: TransactionId, idx: Long): EventId =
    EventIdFormatter.fromTransactionId(txid, NodeId(idx.toInt))

  private def create(
      absCid: AbsoluteContractId,
  ): NodeCreate.WithTxValue[AbsoluteContractId] =
    NodeCreate(
      nodeSeed = None,
      coid = absCid,
      coinst = someContractInstance,
      optLocation = None,
      signatories = Set(alice, bob),
      stakeholders = Set(alice, bob),
      key = None
    )

  private def exercise(
      targetCid: AbsoluteContractId,
  ): NodeExercises.WithTxValue[EventId, AbsoluteContractId] =
    NodeExercises(
      nodeSeed = None,
      targetCoid = targetCid,
      templateId = someTemplateId,
      choiceId = Ref.Name.assertFromString("choice"),
      optLocation = None,
      consuming = true,
      actingParties = Set(alice),
      chosenValue =
        VersionedValue(ValueVersions.acceptedVersions.head, ValueText("some choice value")),
      stakeholders = Set(alice, bob),
      signatories = Set(alice, bob),
      children = ImmArray.empty,
      exerciseResult = Some(
        VersionedValue(ValueVersions.acceptedVersions.head, ValueText("some exercise result"))),
      key = None
    )

  private def transaction(
      head: (EventId, GenNode.WithTxValue[EventId, AbsoluteContractId]),
      tail: (EventId, GenNode.WithTxValue[EventId, AbsoluteContractId])*,
  ): GenTransaction.WithTxValue[EventId, AbsoluteContractId] =
    GenTransaction(
      nodes = HashMap(head +: tail: _*),
      roots = ImmArray(head._1, tail.map(_._1): _*),
    )

  @throws[RuntimeException] // if parent is not there or is not an exercise
  private def addChildren(
      tx: GenTransaction.WithTxValue[EventId, AbsoluteContractId],
      parent: EventId,
      head: (EventId, GenNode.WithTxValue[EventId, AbsoluteContractId]),
      tail: (EventId, GenNode.WithTxValue[EventId, AbsoluteContractId])*,
  ): GenTransaction.WithTxValue[EventId, AbsoluteContractId] =
    tx.copy(
      nodes = tx.nodes.updated(
        key = parent,
        value = tx.nodes.get(parent) match {
          case Some(node: NodeExercises.WithTxValue[EventId, AbsoluteContractId]) =>
            node.copy(
              children = node.children.slowAppend(ImmArray(head._1, tail.map(_._1): _*))
            )
          case Some(node) => sys.error(s"Cannot add children to non-exercise node $node")
          case None => sys.error(s"Cannot find $parent")
        }
      ) + head ++ tail
    )

  // All non-transient contracts created in a transaction
  protected def nonTransient(tx: LedgerEntry.Transaction): Set[AbsoluteContractId] =
    tx.transaction.fold(Set.empty[AbsoluteContractId]) {
      case (set, (_, create: NodeCreate.WithTxValue[AbsoluteContractId])) =>
        set + create.coid
      case (set, (_, exercise: Node.NodeExercises.WithTxValue[EventId, AbsoluteContractId]))
          if exercise.consuming =>
        set - exercise.targetCoid
      case (set, _) =>
        set
    }

  protected def singleCreate: (Offset, LedgerEntry.Transaction) = {
    val offset = nextOffset()
    val id = offset.toLong
    val txId = s"trId$id"
    val absCid = AbsoluteContractId(s"cId$id")
    val let = Instant.now
    val eid = event(txId, id)
    offset -> LedgerEntry.Transaction(
      Some(s"commandId$id"),
      txId,
      Some("appID1"),
      Some("Alice"),
      Some("workflowId"),
      let,
      let,
      transaction(eid -> create(absCid)),
      Map(eid -> Set("Alice", "Bob"))
    )
  }

  protected def singleExercise(
      targetCid: AbsoluteContractId,
  ): (Offset, LedgerEntry.Transaction) = {
    val offset = nextOffset()
    val id = offset.toLong
    val txId = s"trId$id"
    val let = Instant.now
    val eid = event(txId, id)
    offset -> LedgerEntry.Transaction(
      Some(s"commandId$id"),
      txId,
      Some("appID1"),
      Some("Alice"),
      Some("workflowId"),
      let,
      let,
      transaction(eid -> exercise(targetCid)),
      Map(eid -> Set("Alice", "Bob"))
    )
  }

  protected def fullyTransient: (Offset, LedgerEntry.Transaction) = {
    val txId = UUID.randomUUID().toString
    val absCid = AbsoluteContractId(UUID.randomUUID().toString)
    val let = Instant.now
    val createId = event(txId, 0)
    val exerciseId = event(txId, 1)
    nextOffset() -> LedgerEntry.Transaction(
      Some(UUID.randomUUID().toString),
      txId,
      Some("appID1"),
      Some("Alice"),
      Some("workflowId"),
      let,
      let,
      transaction(
        createId -> create(absCid),
        exerciseId -> exercise(absCid)
      ),
      Map(
        createId -> Set("Alice", "Bob"),
        exerciseId -> Set("Alice", "Bob"),
      )
    )
  }

  /**
    * Creates the following transaction
    *
    * Create A --> Exercise A
    *              |        |
    *              |        |
    *              v        v
    *           Create B  Exercise B
    *
    * A is visible to Charlie
    * B is visible to Alice, Bob and Charlie
    *
    */
  protected def withChildren: (Offset, LedgerEntry.Transaction) = {
    val txId = UUID.randomUUID().toString
    val absCid1 = AbsoluteContractId(UUID.randomUUID().toString)
    val absCid2 = AbsoluteContractId(UUID.randomUUID().toString)
    val let = Instant.now
    val createId = event(txId, 0)
    val exerciseId = event(txId, 1)
    val childCreateId = event(txId, 2)
    val childExerciseId = event(txId, 3)
    nextOffset() -> LedgerEntry.Transaction(
      Some(UUID.randomUUID().toString),
      txId,
      Some("appID1"),
      Some(charlie),
      Some("workflowId"),
      let,
      let,
      addChildren(
        tx = transaction(
          createId -> create(absCid1).copy(
            signatories = Set(charlie),
            stakeholders = Set(charlie),
          ),
          exerciseId -> exercise(absCid1).copy(
            actingParties = Set(charlie),
            signatories = Set(charlie),
            stakeholders = Set(charlie),
          ),
        ),
        parent = exerciseId,
        childCreateId -> create(absCid2),
        childExerciseId -> exercise(absCid2)
      ),
      Map(
        createId -> Set(charlie),
        exerciseId -> Set(charlie),
        childCreateId -> Set(alice, bob, charlie),
        childExerciseId -> Set(alice, bob, charlie),
      )
    )
  }

  protected final def store(offsetAndTx: (Offset, LedgerEntry.Transaction))(
      implicit ec: ExecutionContext): Future[(Offset, LedgerEntry.Transaction)] = {
    ledgerDao
      .storeLedgerEntry(
        offsetAndTx._1,
        PersistenceEntry.Transaction(offsetAndTx._2, Map.empty, List.empty))
      .map(_ => offsetAndTx)
  }

  /** A transaction that creates the given key */
  protected final def txCreateContractWithKey(
      let: Instant,
      offset: Offset,
      party: Party,
      key: String): PersistenceEntry.Transaction = {
    val id = offset.toLong
    PersistenceEntry.Transaction(
      LedgerEntry.Transaction(
        Some(s"commandId$id"),
        s"transactionId$id",
        Some("applicationId"),
        Some(party),
        Some("workflowId"),
        let,
        let,
        GenTransaction(
          HashMap(
            event(s"transactionId$id", id) -> NodeCreate(
              nodeSeed = None,
              coid = AbsoluteContractId(s"contractId$id"),
              coinst = someContractInstance,
              optLocation = None,
              signatories = Set(party),
              stakeholders = Set(party),
              key = Some(
                KeyWithMaintainers(
                  VersionedValue(ValueVersions.acceptedVersions.head, ValueText(key)),
                  Set(party)))
            )),
          ImmArray(event(s"transactionId$id", id)),
        ),
        Map(event(s"transactionId$id", id) -> Set(party))
      ),
      Map.empty,
      List.empty
    )
  }

  /** A transaction that archives the given contract with the given key */
  protected final def txArchiveContract(
      let: Instant,
      offset: Offset,
      party: Party,
      cid: Offset,
      key: String): PersistenceEntry.Transaction = {
    val id = offset.toLong
    PersistenceEntry.Transaction(
      LedgerEntry.Transaction(
        Some(s"commandId$id"),
        s"transactionId$id",
        Some("applicationId"),
        Some(party),
        Some("workflowId"),
        let,
        let,
        GenTransaction(
          HashMap(
            event(s"transactionId$id", id) -> NodeExercises(
              nodeSeed = None,
              targetCoid = AbsoluteContractId(s"contractId${cid.toLong}"),
              templateId = someTemplateId,
              choiceId = Ref.ChoiceName.assertFromString("Archive"),
              optLocation = None,
              consuming = true,
              actingParties = Set(party),
              chosenValue = VersionedValue(ValueVersions.acceptedVersions.head, ValueUnit),
              stakeholders = Set(party),
              signatories = Set(party),
              controllers = Set(party),
              children = ImmArray.empty,
              exerciseResult = Some(VersionedValue(ValueVersions.acceptedVersions.head, ValueUnit)),
              key = Some(
                KeyWithMaintainers(
                  VersionedValue(ValueVersions.acceptedVersions.head, ValueText(key)),
                  Set(party)))
            )),
          ImmArray(event(s"transactionId$id", id)),
        ),
        Map(event(s"transactionId$id", id) -> Set(party))
      ),
      Map.empty,
      List.empty
    )
  }

  /** A transaction that looks up a key */
  protected final def txLookupByKey(
      let: Instant,
      offset: Offset,
      party: Party,
      key: String,
      result: Option[Offset]): PersistenceEntry.Transaction = {
    val id = offset.toLong
    PersistenceEntry.Transaction(
      LedgerEntry.Transaction(
        Some(s"commandId$id"),
        s"transactionId$id",
        Some("applicationId"),
        Some(party),
        Some("workflowId"),
        let,
        let,
        GenTransaction(
          HashMap(
            event(s"transactionId$id", id) -> NodeLookupByKey(
              someTemplateId,
              None,
              KeyWithMaintainers(
                VersionedValue(ValueVersions.acceptedVersions.head, ValueText(key)),
                Set(party)),
              result.map(id => AbsoluteContractId(s"contractId${id.toLong}")),
            )),
          ImmArray(event(s"transactionId$id", id)),
        ),
        Map(event(s"transactionId$id", id) -> Set(party))
      ),
      Map.empty,
      List.empty
    )
  }

  /** A transaction that fetches a contract Id */
  protected final def txFetch(
      let: Instant,
      offset: Offset,
      party: Party,
      cid: Offset): PersistenceEntry.Transaction = {
    val id = offset.toLong
    PersistenceEntry.Transaction(
      LedgerEntry.Transaction(
        Some(s"commandId$id"),
        s"transactionId$id",
        Some("applicationId"),
        Some(party),
        Some("workflowId"),
        let,
        let,
        GenTransaction(
          HashMap(
            event(s"transactionId$id", id) -> NodeFetch(
              coid = AbsoluteContractId(s"contractId${cid.toLong}"),
              templateId = someTemplateId,
              optLocation = None,
              actingParties = Some(Set(party)),
              signatories = Set(party),
              stakeholders = Set(party),
            )),
          ImmArray(event(s"transactionId$id", id)),
        ),
        Map(event(s"transactionId$id", id) -> Set(party))
      ),
      Map.empty,
      List.empty
    )
  }

  protected final def emptyTxWithDivulgedContracts(
      offset: Offset,
      let: Instant,
  ): PersistenceEntry.Transaction = {
    val id = offset.toLong
    PersistenceEntry.Transaction(
      LedgerEntry.Transaction(
        Some(s"commandId$id"),
        s"transactionId$id",
        Some("applicationId"),
        Some(alice),
        Some("workflowId"),
        let,
        let,
        GenTransaction(HashMap.empty, ImmArray.empty),
        Map.empty
      ),
      Map(AbsoluteContractId(s"contractId$id") -> Set(bob)),
      List(AbsoluteContractId(s"contractId$id") -> someContractInstance)
    )
  }

}
