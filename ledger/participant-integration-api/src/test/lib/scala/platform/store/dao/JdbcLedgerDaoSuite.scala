// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.io.File
import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.stream.scaladsl.Sink
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.archive.DarReader
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.Node._
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{
  BlindingInfo,
  CommittedTransaction,
  Node,
  NodeId,
  TransactionVersions
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInst, ValueRecord, ValueText, ValueUnit}
import com.daml.logging.LoggingContext
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.AsyncTestSuite

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Success, Try}

private[dao] trait JdbcLedgerDaoSuite extends JdbcLedgerDaoBackend {
  this: AsyncTestSuite =>

  protected implicit final val loggingContext: LoggingContext = LoggingContext.ForTesting

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
  protected final val david = Party.assertFromString("David")

  protected final val defaultAppId = "default-app-id"
  protected final val defaultWorkflowId = "default-workflow-id"
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
  protected final val someContractKey = someValueText
  protected final val someContractInstance = ContractInst(
    someTemplateId,
    someValueRecord,
    someAgreement
  )
  protected final val someVersionedContractInstance =
    TransactionBuilder().versionContract(someContractInstance)

  protected final val defaultConfig = v1.Configuration(
    generation = 0,
    timeModel = v1.TimeModel.reasonableDefault,
    Duration.ofDays(1),
  )

  private val reader = DarReader { (_, stream) =>
    Try(DamlLf.Archive.parseFrom(stream))
  }
  private val Success(dar) =
    reader.readArchiveFromFile(new File(rlocation("ledger/test-common/model-tests.dar")))
  private val now = Instant.now()

  protected final val packages: List[(DamlLf.Archive, v2.PackageDetails)] =
    dar.all.map(dar => dar -> v2.PackageDetails(dar.getSerializedSize.toLong, now, None))

  protected implicit def toParty(s: String): Party = Party.assertFromString(s)

  protected implicit def toLedgerString(s: String): Ref.LedgerString =
    Ref.LedgerString.assertFromString(s)

  protected final def create(
      absCid: ContractId,
      signatories: Set[Party] = Set(alice, bob),
  ): NodeCreate[ContractId, Value[ContractId]] =
    NodeCreate(
      coid = absCid,
      coinst = someContractInstance,
      optLocation = None,
      signatories = signatories,
      stakeholders = signatories,
      key = None,
      version = TransactionVersions.minVersion,
    )

  protected final def createWithStakeholders(
      absCid: ContractId,
      signatories: Set[Party],
      stakeholders: Set[Party],
  ): NodeCreate[ContractId, Value[ContractId]] =
    NodeCreate(
      coid = absCid,
      coinst = someContractInstance,
      optLocation = None,
      signatories = signatories,
      stakeholders = stakeholders,
      key = None,
      version = TransactionVersions.minVersion,
    )

  private def exercise(
      targetCid: ContractId,
  ): NodeExercises[NodeId, ContractId, Value[ContractId]] =
    NodeExercises(
      targetCoid = targetCid,
      templateId = someTemplateId,
      choiceId = Ref.Name.assertFromString("choice"),
      optLocation = None,
      consuming = true,
      actingParties = Set(alice),
      chosenValue = ValueText("some choice value"),
      stakeholders = Set(alice, bob),
      signatories = Set(alice, bob),
      choiceObservers = Set.empty,
      children = ImmArray.empty,
      exerciseResult = Some(ValueText("some exercise result")),
      key = None,
      byKey = false,
      version = TransactionVersions.minVersion,
    )

  // All non-transient contracts created in a transaction
  protected def nonTransient(tx: LedgerEntry.Transaction): Set[ContractId] =
    tx.transaction.fold(Set.empty[ContractId]) {
      case (set, (_, create: NodeCreate.WithTxValue[ContractId])) =>
        set + create.coid
      case (set, (_, exercise: Node.NodeExercises.WithTxValue[NodeId, ContractId]))
          if exercise.consuming =>
        set - exercise.targetCoid
      case (set, _) =>
        set
    }

  protected final def singleCreateP(create: ContractId => NodeCreate[ContractId, Value[ContractId]])
    : (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val cid = txBuilder.newCid
    val creation = create(cid)
    val eid = txBuilder.add(creation)
    val offset = nextOffset()
    val id = offset.toLong
    val let = Instant.now
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"commandId$id"),
      transactionId = s"trId$id",
      applicationId = Some("appID1"),
      actAs = List(alice),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(eid -> (creation.signatories union creation.stakeholders))
    )
  }

  protected final def singleCreate: (Offset, LedgerEntry.Transaction) =
    singleCreateP(create(_))

  protected final def multiPartySingleCreateP(
      create: ContractId => NodeCreate[ContractId, Value[ContractId]],
      actAs: List[Party],
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val cid = txBuilder.newCid
    val creation = create(cid)
    val eid = txBuilder.add(creation)
    val offset = nextOffset()
    val id = offset.toLong
    val let = Instant.now
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"commandId$id"),
      transactionId = s"trId$id",
      applicationId = Some("appID1"),
      actAs = actAs,
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(eid -> (creation.signatories union creation.stakeholders))
    )
  }

  protected final def multiPartySingleCreate: (Offset, LedgerEntry.Transaction) =
    multiPartySingleCreateP(create(_, Set(alice, bob)), List(alice, bob))

  protected def divulgeAlreadyCommittedContract(
      id: ContractId,
      divulgees: Set[Party],
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val exerciseId = txBuilder.add(
      NodeExercises(
        targetCoid = id,
        templateId = someTemplateId,
        choiceId = Ref.ChoiceName.assertFromString("someChoice"),
        optLocation = None,
        consuming = false,
        actingParties = Set(alice),
        chosenValue = ValueUnit,
        stakeholders = divulgees,
        signatories = divulgees,
        choiceObservers = Set.empty,
        children = ImmArray.empty,
        exerciseResult = Some(ValueUnit),
        key = None,
        byKey = false,
        version = TransactionVersions.minVersion,
      )
    )
    txBuilder.add(
      NodeFetch(
        coid = id,
        templateId = someTemplateId,
        optLocation = None,
        actingParties = divulgees,
        signatories = Set(alice),
        stakeholders = Set(alice),
        None,
        byKey = false,
        version = TransactionVersions.minVersion,
      ),
      exerciseId,
    )
    val offset = nextOffset()
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"just-divulged-${id.coid}"),
      transactionId = s"trId${id.coid}",
      applicationId = Some("appID1"),
      actAs = List(divulgees.head),
      workflowId = None,
      ledgerEffectiveTime = Instant.now,
      recordedAt = Instant.now,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map.empty,
    )
  }

  protected def singleExercise(
      targetCid: ContractId,
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val nid = txBuilder.add(exercise(targetCid))
    val offset = nextOffset()
    val id = offset.toLong
    val let = Instant.now
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"commandId$id"),
      transactionId = s"trId$id",
      applicationId = Some("appID1"),
      actAs = List("Alice"),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = CommittedTransaction(txBuilder.buildCommitted()),
      explicitDisclosure = Map(nid -> Set("Alice", "Bob"))
    )
  }

  protected def multiPartySingleExercise(
      targetCid: ContractId,
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val nid = txBuilder.add(exercise(targetCid))
    val offset = nextOffset()
    val id = offset.toLong
    val let = Instant.now
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"commandId$id"),
      transactionId = s"trId$id",
      applicationId = Some("appID1"),
      actAs = List(alice, bob, charlie),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = CommittedTransaction(txBuilder.buildCommitted()),
      explicitDisclosure = Map(nid -> Set(alice, bob))
    )
  }

  protected def singleNonConsumingExercise(
      targetCid: ContractId,
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val nid = txBuilder.add(exercise(targetCid).copy(consuming = false))
    val offset = nextOffset()
    val id = offset.toLong
    val let = Instant.now
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"commandId$id"),
      transactionId = s"trId$id",
      applicationId = Some("appID1"),
      actAs = List("Alice"),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(nid -> Set("Alice", "Bob"))
    )
  }

  protected def exerciseWithChild(
      targetCid: ContractId,
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val exerciseId = txBuilder.add(exercise(targetCid))
    val childId = txBuilder.add(create(txBuilder.newCid), exerciseId)
    val tx = CommittedTransaction(txBuilder.build())
    val offset = nextOffset()
    val id = offset.toLong
    val txId = s"trId$id"
    val let = Instant.now
    offset -> LedgerEntry.Transaction(
      Some(s"commandId$id"),
      txId,
      Some("appID1"),
      List("Alice"),
      Some("workflowId"),
      let,
      let,
      CommittedTransaction(tx),
      Map(exerciseId -> Set("Alice", "Bob"), childId -> Set("Alice", "Bob"))
    )
  }

  protected def fullyTransient: (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val cid = txBuilder.newCid
    val createId = txBuilder.add(create(cid))
    val exerciseId = txBuilder.add(exercise(cid))
    val let = Instant.now
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID().toString,
      applicationId = Some("appID1"),
      actAs = List(alice),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(
        createId -> Set(alice, bob),
        exerciseId -> Set(alice, bob),
      )
    )
  }

  // The transient contract is divulged to `charlie` as a non-stakeholder actor on the
  // root exercise node that causes the creation of a transient contract
  protected def fullyTransientWithChildren: (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val root = txBuilder.newCid
    val transient = txBuilder.newCid
    val rootCreateId = txBuilder.add(create(root))
    val rootExerciseId = txBuilder.add(exercise(root).copy(actingParties = Set(charlie)))
    val createTransientId = txBuilder.add(create(transient), rootExerciseId)
    val consumeTransientId = txBuilder.add(exercise(transient), rootExerciseId)
    val let = Instant.now
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID.toString),
      transactionId = UUID.randomUUID().toString,
      applicationId = Some("appID1"),
      actAs = List(alice),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(
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
    val txBuilder = TransactionBuilder()
    val createId = txBuilder.add(
      create(txBuilder.newCid).copy(
        signatories = Set(charlie),
        stakeholders = Set(charlie),
      ))
    val exerciseId = txBuilder.add(
      exercise(txBuilder.newCid).copy(
        actingParties = Set(charlie),
        signatories = Set(charlie),
        stakeholders = Set(charlie),
      )
    )
    val childCreateId1 = txBuilder.add(
      create(txBuilder.newCid),
      exerciseId,
    )
    val childCreateId2 = txBuilder.add(
      create(txBuilder.newCid),
      exerciseId,
    )
    val let = Instant.now
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID().toString,
      applicationId = Some("appID1"),
      actAs = List(charlie),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(
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
    val txBuilder = TransactionBuilder()
    val disclosure = for {
      entry <- signatoriesAndTemplates
      (signatory, template) = entry
      contract = create(txBuilder.newCid)
      parties = Set[Party](operator, signatory)
      nodeId = txBuilder.add(
        contract.copy(
          signatories = parties,
          stakeholders = parties,
          coinst = contract.coinst.copy(template = Identifier.assertFromString(template)),
        ))
    } yield nodeId -> parties
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some("appID1"),
      actAs = List(operator),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = Instant.now,
      recordedAt = Instant.now,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = disclosure.toMap,
    )
  }

  protected final def store(
      divulgedContracts: Map[(ContractId, v1.ContractInst), Set[Party]],
      blindingInfo: Option[BlindingInfo],
      offsetAndTx: (Offset, LedgerEntry.Transaction),
  ): Future[(Offset, LedgerEntry.Transaction)] = {
    val (offset, entry) = offsetAndTx
    val submitterInfo =
      for (actAs <- if (entry.actAs.isEmpty) None else Some(entry.actAs); app <- entry.applicationId;
        cmd <- entry.commandId) yield v1.SubmitterInfo(actAs, app, cmd, Instant.EPOCH)
    val committedTransaction = CommittedTransaction(entry.transaction)
    val ledgerEffectiveTime = entry.ledgerEffectiveTime
    val divulged = divulgedContracts.keysIterator.map(c => v1.DivulgedContract(c._1, c._2)).toList
    val preparedTransactionInsert = ledgerDao.prepareTransactionInsert(
      submitterInfo,
      entry.workflowId,
      entry.transactionId,
      ledgerEffectiveTime,
      offset,
      committedTransaction,
      divulged,
      blindingInfo,
    )
    ledgerDao
      .storeTransaction(
        preparedInsert = preparedTransactionInsert,
        submitterInfo = submitterInfo,
        transactionId = entry.transactionId,
        transaction = committedTransaction,
        recordTime = entry.recordedAt,
        ledgerEffectiveTime = ledgerEffectiveTime,
        offset = offset,
        divulged = divulged,
        blindingInfo = blindingInfo,
      )
      .map(_ => offsetAndTx)
  }

  protected final def store(
      offsetAndTx: (Offset, LedgerEntry.Transaction),
  ): Future[(Offset, LedgerEntry.Transaction)] =
    store(divulgedContracts = Map.empty, blindingInfo = None, offsetAndTx)

  protected final def storeSync(
      commands: Vector[(Offset, LedgerEntry.Transaction)],
  ): Future[Vector[(Offset, LedgerEntry.Transaction)]] = {

    import JdbcLedgerDaoSuite._
    import scalaz.std.scalaFuture._
    import scalaz.std.vector._

    // force synchronous future processing with Free monad
    // to provide the guarantees that all transactions persisted in the specified order
    commands traverseFM store
  }

  /** A transaction that creates the given key */
  protected final def txCreateContractWithKey(
      party: Party,
      key: String,
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val createNodeId = txBuilder.add(
      NodeCreate(
        coid = txBuilder.newCid,
        coinst = someContractInstance,
        optLocation = None,
        signatories = Set(party),
        stakeholders = Set(party),
        key = Some(KeyWithMaintainers(ValueText(key), Set(party))),
        version = TransactionVersions.minVersion,
      ))
    nextOffset() ->
      LedgerEntry.Transaction(
        commandId = Some(UUID.randomUUID().toString),
        transactionId = UUID.randomUUID.toString,
        applicationId = Some(defaultAppId),
        actAs = List(party),
        workflowId = Some(defaultWorkflowId),
        ledgerEffectiveTime = Instant.now,
        recordedAt = Instant.now,
        transaction = txBuilder.buildCommitted(),
        explicitDisclosure = Map(createNodeId -> Set(party))
      )
  }

  /** A transaction that archives the given contract with the given key */
  protected final def txArchiveContract(
      party: Party,
      contract: (ContractId, Option[String]),
  ): (Offset, LedgerEntry.Transaction) = {
    val (contractId, maybeKey) = contract
    val txBuilder = TransactionBuilder()
    val archiveNodeId = txBuilder.add(
      NodeExercises(
        targetCoid = contractId,
        templateId = someTemplateId,
        choiceId = Ref.ChoiceName.assertFromString("Archive"),
        optLocation = None,
        consuming = true,
        actingParties = Set(party),
        chosenValue = ValueUnit,
        stakeholders = Set(party),
        signatories = Set(party),
        choiceObservers = Set.empty,
        children = ImmArray.empty,
        exerciseResult = Some(ValueUnit),
        key = maybeKey.map(k => KeyWithMaintainers(ValueText(k), Set(party))),
        byKey = false,
        version = TransactionVersions.minVersion,
      ))
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some(defaultAppId),
      actAs = List(party),
      workflowId = Some(defaultWorkflowId),
      ledgerEffectiveTime = Instant.now,
      recordedAt = Instant.now,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(archiveNodeId -> Set(party))
    )
  }

  /** A transaction that looks up a key */
  protected final def txLookupByKey(
      party: Party,
      key: String,
      result: Option[ContractId],
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val lookupByKeyNodeId = txBuilder.add(
      NodeLookupByKey(
        someTemplateId,
        None,
        KeyWithMaintainers(ValueText(key), Set(party)),
        result,
        version = TransactionVersions.minVersion,
      ))
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some(defaultAppId),
      actAs = List(party),
      workflowId = Some(defaultWorkflowId),
      ledgerEffectiveTime = Instant.now(),
      recordedAt = Instant.now(),
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(lookupByKeyNodeId -> Set(party))
    )
  }

  protected final def txFetch(
      party: Party,
      contractId: ContractId,
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val fetchNodeId = txBuilder.add(
      NodeFetch(
        coid = contractId,
        templateId = someTemplateId,
        optLocation = None,
        actingParties = Set(party),
        signatories = Set(party),
        stakeholders = Set(party),
        None,
        byKey = false,
        version = TransactionVersions.minVersion,
      ))
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some(defaultAppId),
      actAs = List(party),
      workflowId = Some(defaultWorkflowId),
      ledgerEffectiveTime = Instant.now(),
      recordedAt = Instant.now(),
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(fetchNodeId -> Set(party))
    )
  }

  protected final def emptyTransaction(party: Party): (Offset, LedgerEntry.Transaction) =
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some(defaultAppId),
      actAs = List(party),
      workflowId = Some(defaultWorkflowId),
      ledgerEffectiveTime = Instant.now(),
      recordedAt = Instant.now(),
      transaction = TransactionBuilder.EmptyCommitted,
      explicitDisclosure = Map.empty,
    )

  // Returns the command ids and status of completed commands between two offsets
  protected def getCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: String,
      parties: Set[Party],
  ): Future[Seq[(String, Int)]] =
    ledgerDao.completions
      .getCommandCompletions(startExclusive, endInclusive, applicationId, parties)
      .map(_._2.completions.head)
      .map(c => c.commandId -> c.status.get.code)
      .runWith(Sink.seq)

}

object JdbcLedgerDaoSuite {

  import scalaz.syntax.traverse._
  import scalaz.{Free, Monad, NaturalTransformation, Traverse}

  import scala.language.higherKinds

  implicit final class `TraverseFM Ops`[T[_], A](private val self: T[A]) extends AnyVal {

    /** Like `traverse`, but guarantees that
      *
      *  - `f` is evaluated left-to-right, and
      *  - `B` from the preceding element is evaluated before `f` is invoked for
      * the subsequent `A`.
      */
    def traverseFM[F[_]: Monad, B](f: A => F[B])(implicit T: Traverse[T]): F[T[B]] =
      self
        .traverse(a => Free suspend (Free liftF f(a)))
        .foldMap(NaturalTransformation.refl)
  }
}
