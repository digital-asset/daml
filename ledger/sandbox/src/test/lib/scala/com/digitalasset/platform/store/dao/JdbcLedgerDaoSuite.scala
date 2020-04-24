// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.io.File
import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1.{AbsoluteContractInst, Configuration, Offset, TimeModel}
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.lf.archive.DarReader
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.Node._
import com.daml.lf.transaction.{GenTransaction, Node}
import com.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractInst,
  NodeId,
  ValueRecord,
  ValueText,
  ValueUnit,
  VersionedValue
}
import com.daml.lf.value.ValueVersions
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.{EventId, TransactionId}
import com.daml.platform.events.EventIdFormatter
import com.daml.platform.store.PersistenceEntry
import com.daml.platform.store.entries.LedgerEntry
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
    val absCid = AbsoluteContractId.assertFromString(s"#cId$id")
    val let = Instant.now
    val eid = event(txId, id)
    offset -> LedgerEntry.Transaction(
      Some(s"commandId$id"),
      txId,
      Some("appID1"),
      Some(alice),
      Some("workflowId"),
      let,
      let,
      transaction(eid -> create(absCid)),
      Map(eid -> Set("Alice", "Bob"))
    )
  }

  protected def divulgeAlreadyCommittedContract(
      id: AbsoluteContractId,
      divulgees: Set[Party],
  ): (Offset, LedgerEntry.Transaction) = {
    val offset = nextOffset()
    val txId = s"trId${id.coid}"
    val exerciseId = event(txId, 0L)
    val fetchEventId = event(txId, 1L)
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"just-divulged-${id.coid}"),
      transactionId = txId,
      Some("appID1"),
      Some(divulgees.head),
      workflowId = None,
      ledgerEffectiveTime = Instant.now,
      recordedAt = Instant.now,
      transaction = GenTransaction(
        HashMap(
          exerciseId -> NodeExercises(
            targetCoid = id,
            templateId = someTemplateId,
            choiceId = Ref.ChoiceName.assertFromString("someChoice"),
            optLocation = None,
            consuming = false,
            actingParties = Set(alice),
            chosenValue = VersionedValue(ValueVersions.acceptedVersions.head, ValueUnit),
            stakeholders = divulgees,
            signatories = divulgees,
            children = ImmArray(fetchEventId),
            exerciseResult = Some(VersionedValue(ValueVersions.acceptedVersions.head, ValueUnit)),
            key = None,
          ),
          fetchEventId -> NodeFetch(
            coid = id,
            templateId = someTemplateId,
            optLocation = None,
            actingParties = Some(divulgees),
            signatories = Set(alice),
            stakeholders = Set(alice),
            None,
          )
        ),
        ImmArray(exerciseId),
      ),
      explicitDisclosure = Map.empty,
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
    val absCid = AbsoluteContractId.assertFromString("#" + UUID.randomUUID().toString)
    val let = Instant.now
    val createId = event(txId, 0)
    val exerciseId = event(txId, 1)
    nextOffset() -> LedgerEntry.Transaction(
      Some(UUID.randomUUID().toString),
      txId,
      Some("appID1"),
      Some(alice),
      Some("workflowId"),
      let,
      let,
      transaction(
        createId -> create(absCid),
        exerciseId -> exercise(absCid)
      ),
      Map(
        createId -> Set(alice, bob),
        exerciseId -> Set(alice, bob),
      )
    )
  }

  // The transient contract is divulged to `charlie` as a non-stakeholder actor on the
  // root exercise node that causes the creation of a transient contract
  protected def fullyTransientWithChildren: (Offset, LedgerEntry.Transaction) = {
    val txId = UUID.randomUUID().toString
    val root = AbsoluteContractId.assertFromString(s"#root-transient-${UUID.randomUUID}")
    val transient = AbsoluteContractId.assertFromString(s"#child-transient-${UUID.randomUUID}")
    val let = Instant.now
    val rootCreateId = event(txId, 0)
    val rootExerciseId = event(txId, 1)
    val createTransientId = event(txId, 2)
    val consumeTransientId = event(txId, 3)
    nextOffset() -> LedgerEntry.Transaction(
      Some(UUID.randomUUID.toString),
      txId,
      Some("appID1"),
      Some(alice),
      Some("workflowId"),
      let,
      let,
      addChildren(
        tx = transaction(
          rootCreateId -> create(root),
          rootExerciseId -> exercise(root).copy(actingParties = Set(charlie)),
        ),
        parent = rootExerciseId,
        createTransientId -> create(transient),
        consumeTransientId -> exercise(transient),
      ),
      Map(
        rootCreateId -> Set(alice, bob),
        rootExerciseId -> Set(alice, bob, charlie),
        createTransientId -> Set(alice, bob, charlie),
        consumeTransientId -> Set(alice, bob, charlie),
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
    *           Create B  Create C
    *
    * A is visible to Charlie
    * B is visible to Alice and Charlie
    * C is visible to Bob and Charlie
    *
    */
  protected def partiallyVisible: (Offset, LedgerEntry.Transaction) = {
    val txId = UUID.randomUUID().toString
    val absCid1 = AbsoluteContractId.assertFromString(s"#absCid1-${UUID.randomUUID}")
    val absCid2 = AbsoluteContractId.assertFromString(s"#absCid2-${UUID.randomUUID}")
    val absCid3 = AbsoluteContractId.assertFromString(s"#absCid3-${UUID.randomUUID}")
    val let = Instant.now
    val createId = event(txId, 0)
    val exerciseId = event(txId, 1)
    val childCreateId1 = event(txId, 2)
    val childCreateId2 = event(txId, 3)
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
        childCreateId1 -> create(absCid2),
        childCreateId2 -> create(absCid3),
      ),
      Map(
        createId -> Set(charlie),
        exerciseId -> Set(charlie),
        childCreateId1 -> Set(alice, charlie),
        childCreateId2 -> Set(bob, charlie),
      )
    )
  }

  /**
    * Creates a transactions with multiple top-level creates.
    *
    * Every contract will be signed by a fixed "operator" and each contract will have a
    * further signatory and a template as defined by signatoriesAndTemplates.
    *
    * @throws IllegalArgumentException if signatoryAndTemplate is empty
    */
  protected def multipleCreates(
      operator: String,
      signatoriesAndTemplates: Seq[(String, String)],
  ): (Offset, LedgerEntry.Transaction) = {
    require(signatoriesAndTemplates.nonEmpty, "multipleCreates cannot create empty transactions")
    val transactionId = UUID.randomUUID.toString
    val nodes =
      for (((signatory, template), index) <- signatoriesAndTemplates.zipWithIndex) yield {
        val contract = create(AbsoluteContractId.assertFromString("#" + UUID.randomUUID.toString))
        event(transactionId, index.toLong) -> contract.copy(
          signatories = Set(operator, signatory),
          stakeholders = Set(operator, signatory),
          coinst = contract.coinst.copy(template = Identifier.assertFromString(template)),
        )
      }
    val disclosure = Map(nodes.map { case (event, contract) => event -> contract.signatories }: _*)
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = transactionId,
      applicationId = Some("appID1"),
      submittingParty = Some(operator),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = Instant.now,
      recordedAt = Instant.now,
      transaction = GenTransaction(nodes = HashMap(nodes: _*), roots = ImmArray(nodes.map(_._1))),
      explicitDisclosure = disclosure,
    )
  }

  protected final def store(
      divulgedContracts: Map[(AbsoluteContractId, AbsoluteContractInst), Set[Party]],
      offsetAndTx: (Offset, LedgerEntry.Transaction))(
      implicit ec: ExecutionContext): Future[(Offset, LedgerEntry.Transaction)] =
    ledgerDao
      .storeLedgerEntry(
        offset = offsetAndTx._1,
        PersistenceEntry.Transaction(
          entry = offsetAndTx._2,
          globalDivulgence = divulgedContracts.map { case ((id, _), witnesses) => id -> witnesses },
          divulgedContracts = divulgedContracts.keysIterator.toList,
        )
      )
      .map(_ => offsetAndTx)

  protected final def store(offsetAndTx: (Offset, LedgerEntry.Transaction))(
      implicit ec: ExecutionContext): Future[(Offset, LedgerEntry.Transaction)] =
    store(divulgedContracts = Map.empty, offsetAndTx)

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
              coid = AbsoluteContractId.assertFromString(s"#contractId$id"),
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
              targetCoid = AbsoluteContractId.assertFromString(s"#contractId${cid.toLong}"),
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
              result.map(id => AbsoluteContractId.assertFromString(s"#contractId${id.toLong}")),
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
              coid = AbsoluteContractId.assertFromString(s"#contractId${cid.toLong}"),
              templateId = someTemplateId,
              optLocation = None,
              actingParties = Some(Set(party)),
              signatories = Set(party),
              stakeholders = Set(party),
              None,
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
      Map(AbsoluteContractId.assertFromString(s"#contractId$id") -> Set(bob)),
      List(AbsoluteContractId.assertFromString(s"#contractId$id") -> someContractInstance)
    )
  }

}
