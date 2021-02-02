// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.io.File
import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import akka.stream.scaladsl.Sink
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{DivulgedContract, Offset, SubmitterInfo}
import com.daml.lf.archive.DarReader
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.lf.data.{FrontStack, ImmArray, Ref, Time}
import com.daml.lf.transaction.Node._
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction._
import com.daml.lf.value.{Value => LfValue}
import com.daml.lf.value.Value.{ContractId, ContractInst}
import com.daml.logging.LoggingContext
import com.daml.platform.indexer.OffsetStep
import com.daml.platform.store.dao.events.TransactionsWriter
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.AsyncTestSuite

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Success, Try}

private[dao] trait JdbcLedgerDaoSuite extends JdbcLedgerDaoBackend {
  this: AsyncTestSuite =>

  protected implicit final val loggingContext: LoggingContext = LoggingContext.ForTesting

  val previousOffset: AtomicReference[Option[Offset]] =
    new AtomicReference[Option[Offset]](Option.empty)

  protected final val nextOffset: () => Offset = {
    val base = BigInt(1) << 32
    val counter = new AtomicLong(0)
    () => {
      Offset.fromByteArray((base + counter.getAndIncrement()).toByteArray)
    }
  }

  protected final implicit class OffsetToLong(offset: Offset) {
    def toLong: Long = BigInt(offset.toByteArray).toLong
  }

  private val reader = DarReader { (_, stream) =>
    Try(DamlLf.Archive.parseFrom(stream))
  }
  private val Success(dar) =
    reader.readArchiveFromFile(new File(rlocation("ledger/test-common/model-tests.dar")))
  private val now = Instant.now()

  protected final val packages: List[(DamlLf.Archive, v2.PackageDetails)] =
    dar.all.map(dar => dar -> v2.PackageDetails(dar.getSerializedSize.toLong, now, None))
  private val testPackageId: Ref.PackageId = Ref.PackageId.assertFromString(dar.main.getHash)

  protected final val alice = Party.assertFromString("Alice")
  protected final val bob = Party.assertFromString("Bob")
  protected final val charlie = Party.assertFromString("Charlie")
  protected final val david = Party.assertFromString("David")
  protected final val emma = Party.assertFromString("Emma")

  protected final val defaultAppId = "default-app-id"
  protected final val defaultWorkflowId = "default-workflow-id"
  protected final val someAgreement = "agreement"

  // Note: *identifiers* and *values* defined below MUST correspond to //ledger/test-common/src/main/daml/model/Test.daml
  // This is because some tests request values in verbose mode, which requires filling in missing type information,
  // which in turn requires loading DAML-LF packages with valid DAML-LF types that correspond to the DAML-LF values.
  //
  // On the other hand, *transactions* do not need to correspond to valid transactions that could be produced by the
  // above mentioned DAML code, e.g., signatories/stakeholders may not correspond to the contract/choice arguments.
  // This is because JdbcLedgerDao is only concerned with serialization, and does not verify the DAML ledger model.
  private def testIdentifier(name: String) = Identifier(
    testPackageId,
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString("Test"),
      Ref.DottedName.assertFromString(name),
    ),
  )
  private def recordFieldName(name: String) = Some(Ref.Name.assertFromString(name))
  protected final val someTemplateId = testIdentifier("ParameterShowcase")
  protected final val someValueText = LfValue.ValueText("some text")
  protected final val someValueInt = LfValue.ValueInt64(1)
  protected final val someValueNumeric =
    LfValue.ValueNumeric(com.daml.lf.data.Numeric.assertFromString("1.1"))
  protected final val someNestedOptionalInteger = LfValue.ValueRecord(
    Some(testIdentifier("NestedOptionalInteger")),
    ImmArray(
      Some(Ref.Name.assertFromString("value")) -> LfValue.ValueVariant(
        Some(testIdentifier("OptionalInteger")),
        Ref.Name.assertFromString("SomeInteger"),
        someValueInt,
      )
    ),
  )
  protected final val someContractArgument = LfValue.ValueRecord(
    Some(someTemplateId),
    ImmArray(
      recordFieldName("operator") -> LfValue.ValueParty(alice),
      recordFieldName("integer") -> someValueInt,
      recordFieldName("decimal") -> someValueNumeric,
      recordFieldName("text") -> someValueText,
      recordFieldName("bool") -> LfValue.ValueBool(true),
      recordFieldName("time") -> LfValue.ValueTimestamp(Time.Timestamp.Epoch),
      recordFieldName("nestedOptionalInteger") -> someNestedOptionalInteger,
      recordFieldName("integerList") -> LfValue.ValueList(FrontStack(someValueInt)),
      recordFieldName("optionalText") -> LfValue.ValueOptional(Some(someValueText)),
    ),
  )
  protected final val someChoiceName = Ref.Name.assertFromString("Choice1")
  protected final val someChoiceArgument = LfValue.ValueRecord(
    Some(testIdentifier(someChoiceName)),
    ImmArray(
      recordFieldName("newInteger") -> someValueInt,
      recordFieldName("newDecimal") -> someValueNumeric,
      recordFieldName("newText") -> someValueText,
      recordFieldName("newBool") -> LfValue.ValueBool(true),
      recordFieldName("newTime") -> LfValue.ValueTimestamp(Time.Timestamp.Epoch),
      recordFieldName("newNestedOptionalInteger") -> someNestedOptionalInteger,
      recordFieldName("newIntegerList") -> LfValue.ValueList(FrontStack(someValueInt)),
      recordFieldName("newOptionalText") -> LfValue.ValueOptional(Some(someValueText)),
    ),
  )
  protected final val someChoiceResult =
    LfValue.ValueContractId[ContractId](ContractId.V0.assertFromString("#1"))
  protected final def someContractKey(party: Party, value: String) = LfValue.ValueRecord(
    None,
    ImmArray(
      None -> LfValue.ValueParty(party),
      None -> LfValue.ValueText(value),
    ),
  )
  protected final val someContractInstance = ContractInst(
    someTemplateId,
    someContractArgument,
    someAgreement,
  )
  protected final val someVersionedContractInstance =
    TransactionBuilder().versionContract(someContractInstance)

  protected final val otherTemplateId = testIdentifier("Dummy")
  protected final val otherContractArgument = LfValue.ValueRecord(
    Some(otherTemplateId),
    ImmArray(
      recordFieldName("operator") -> LfValue.ValueParty(alice)
    ),
  )

  protected final val defaultConfig = v1.Configuration(
    generation = 0,
    timeModel = v1.TimeModel.reasonableDefault,
    Duration.ofDays(1),
  )

  private[dao] def store(
      submitterInfo: Option[SubmitterInfo],
      tx: LedgerEntry.Transaction,
      offsetStep: OffsetStep,
      divulgedContracts: List[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
  ): Future[(Offset, LedgerEntry.Transaction)]

  protected implicit def toParty(s: String): Party = Party.assertFromString(s)

  protected implicit def toLedgerString(s: String): Ref.LedgerString =
    Ref.LedgerString.assertFromString(s)

  protected final def create(
      absCid: ContractId,
      signatories: Set[Party] = Set(alice, bob),
      templateId: Identifier = someTemplateId,
      contractArgument: LfValue[ContractId] = someContractArgument,
  ): NodeCreate[ContractId] =
    createNode(absCid, signatories, signatories, None, templateId, contractArgument)

  protected final def createNode(
      absCid: ContractId,
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers[LfValue[ContractId]]] = None,
      templateId: Identifier = someTemplateId,
      contractArgument: LfValue[ContractId] = someContractArgument,
  ): NodeCreate[ContractId] =
    NodeCreate(
      coid = absCid,
      templateId = templateId,
      arg = contractArgument,
      agreementText = someAgreement,
      optLocation = None,
      signatories = signatories,
      stakeholders = stakeholders,
      key = key,
      version = TransactionVersion.minVersion,
    )

  private def exercise(
      targetCid: ContractId
  ): NodeExercises[NodeId, ContractId] =
    NodeExercises(
      targetCoid = targetCid,
      templateId = someTemplateId,
      choiceId = someChoiceName,
      optLocation = None,
      consuming = true,
      actingParties = Set(alice),
      chosenValue = someChoiceArgument,
      stakeholders = Set(alice, bob),
      signatories = Set(alice, bob),
      choiceObservers = Set.empty,
      children = ImmArray.empty,
      exerciseResult = Some(someChoiceResult),
      key = None,
      byKey = false,
      version = TransactionVersion.minVersion,
    )

  // All non-transient contracts created in a transaction
  protected def nonTransient(tx: LedgerEntry.Transaction): Set[ContractId] =
    tx.transaction.fold(Set.empty[ContractId]) {
      case (set, (_, create: NodeCreate[ContractId])) =>
        set + create.coid
      case (set, (_, exercise: Node.NodeExercises[NodeId, ContractId])) if exercise.consuming =>
        set - exercise.targetCoid
      case (set, _) =>
        set
    }

  protected final def singleCreate: (Offset, LedgerEntry.Transaction) =
    singleCreate(create(_))

  protected final def multiPartySingleCreate: (Offset, LedgerEntry.Transaction) =
    singleCreate(create(_, Set(alice, bob)), List(alice, bob))

  protected final def singleCreate(
      create: ContractId => NodeCreate[ContractId],
      actAs: List[Party] = List(alice),
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
      explicitDisclosure = Map(eid -> (creation.signatories union creation.stakeholders)),
    )
  }

  protected final def createAndStoreContract(
      submittingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers[LfValue[ContractId]]],
  ): Future[(Offset, LedgerEntry.Transaction)] =
    store(
      singleCreate(
        create = { cid =>
          createNode(
            absCid = cid,
            signatories = signatories,
            stakeholders = stakeholders,
            key = key,
          )
        },
        actAs = submittingParties.toList,
      )
    )

  protected final def storeCommitedContractDivulgence(
      id: ContractId,
      divulgees: Set[Party],
  ): Future[(Offset, LedgerEntry.Transaction)] =
    store(
      divulgedContracts = Map((id, someVersionedContractInstance) -> divulgees),
      blindingInfo = None,
      offsetAndTx = divulgeAlreadyCommittedContract(id, divulgees),
    )

  protected def divulgeAlreadyCommittedContract(
      id: ContractId,
      divulgees: Set[Party],
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val exerciseId = txBuilder.add(
      NodeExercises(
        targetCoid = id,
        templateId = someTemplateId,
        choiceId = someChoiceName,
        optLocation = None,
        consuming = false,
        actingParties = Set(alice),
        chosenValue = someChoiceArgument,
        stakeholders = divulgees,
        signatories = divulgees,
        choiceObservers = Set.empty,
        children = ImmArray.empty,
        exerciseResult = Some(someChoiceResult),
        key = None,
        byKey = false,
        version = TransactionVersion.minVersion,
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
        version = TransactionVersion.minVersion,
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
      targetCid: ContractId
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
      explicitDisclosure = Map(nid -> Set("Alice", "Bob")),
    )
  }

  protected def multiPartySingleExercise(
      targetCid: ContractId
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
      explicitDisclosure = Map(nid -> Set(alice, bob)),
    )
  }

  protected def singleNonConsumingExercise(
      targetCid: ContractId
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
      explicitDisclosure = Map(nid -> Set("Alice", "Bob")),
    )
  }

  protected def exerciseWithChild(
      targetCid: ContractId
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
      Map(exerciseId -> Set("Alice", "Bob"), childId -> Set("Alice", "Bob")),
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
      ),
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
      ),
    )
  }

  /** Creates the following transaction
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
    */
  protected def partiallyVisible: (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val createId = txBuilder.add(
      create(txBuilder.newCid).copy(
        signatories = Set(charlie),
        stakeholders = Set(charlie),
      )
    )
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
      ),
    )
  }

  /** Creates a transactions with multiple top-level creates.
    *
    * Every contract will be signed by a fixed "operator" and each contract will have a
    * further signatory and a template as defined by signatoriesAndTemplates.
    *
    * @throws IllegalArgumentException if signatoryAndTemplate is empty
    */
  protected def multipleCreates(
      operator: String,
      signatoriesAndTemplates: Seq[(Party, Identifier, LfValue[ContractId])],
  ): (Offset, LedgerEntry.Transaction) = {
    require(signatoriesAndTemplates.nonEmpty, "multipleCreates cannot create empty transactions")
    val txBuilder = TransactionBuilder()
    val disclosure = for {
      entry <- signatoriesAndTemplates
      (signatory, template, argument) = entry
      contract = create(txBuilder.newCid, Set(signatory), template, argument)
      parties = Set[Party](operator, signatory)
      nodeId = txBuilder.add(contract)
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

  protected final def prepareInsert(
      submitterInfo: Option[SubmitterInfo],
      tx: LedgerEntry.Transaction,
      offsetStep: OffsetStep,
      divulgedContracts: List[DivulgedContract] = List.empty,
      blindingInfo: Option[BlindingInfo] = None,
  ): TransactionsWriter.PreparedInsert =
    ledgerDao.prepareTransactionInsert(
      submitterInfo,
      tx.workflowId,
      tx.transactionId,
      tx.ledgerEffectiveTime,
      offsetStep.offset,
      tx.transaction,
      divulgedContracts,
      blindingInfo,
    )

  protected final def store(
      divulgedContracts: Map[(ContractId, v1.ContractInst), Set[Party]],
      blindingInfo: Option[BlindingInfo],
      offsetAndTx: (Offset, LedgerEntry.Transaction),
  ): Future[(Offset, LedgerEntry.Transaction)] =
    storeOffsetStepAndTx(
      divulgedContracts = divulgedContracts,
      blindingInfo = blindingInfo,
      offsetStepAndTx = nextOffsetStep(offsetAndTx._1) -> offsetAndTx._2,
    )

  protected final def storeOffsetStepAndTx(
      divulgedContracts: Map[(ContractId, v1.ContractInst), Set[Party]],
      blindingInfo: Option[BlindingInfo],
      offsetStepAndTx: (OffsetStep, LedgerEntry.Transaction),
  ): Future[(Offset, LedgerEntry.Transaction)] = {
    val (offsetStep, entry) = offsetStepAndTx
    val maybeSubmitterInfo = submitterInfo(entry)
    val divulged = divulgedContracts.keysIterator.map(c => v1.DivulgedContract(c._1, c._2)).toList

    store(maybeSubmitterInfo, entry, offsetStep, divulged, blindingInfo)
  }

  protected def submitterInfo(entry: LedgerEntry.Transaction) =
    for (
      actAs <- if (entry.actAs.isEmpty) None else Some(entry.actAs); app <- entry.applicationId;
      cmd <- entry.commandId
    ) yield v1.SubmitterInfo(actAs, app, cmd, Instant.EPOCH)

  protected final def store(
      offsetAndTx: (Offset, LedgerEntry.Transaction)
  ): Future[(Offset, LedgerEntry.Transaction)] =
    storeOffsetStepAndTx(
      divulgedContracts = Map.empty,
      blindingInfo = None,
      offsetStepAndTx = nextOffsetStep(offsetAndTx._1) -> offsetAndTx._2,
    )

  protected final def storeSync(
      commands: Vector[(Offset, LedgerEntry.Transaction)]
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
      txUuid: Option[String] = None,
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = TransactionBuilder()
    val createNodeId = txBuilder.add(
      NodeCreate(
        coid = txBuilder.newCid,
        templateId = someTemplateId,
        arg = someContractArgument,
        agreementText = someAgreement,
        optLocation = None,
        signatories = Set(party),
        stakeholders = Set(party),
        key = Some(KeyWithMaintainers(someContractKey(party, key), Set(party))),
        version = TransactionVersion.minVersion,
      )
    )
    nextOffset() ->
      LedgerEntry.Transaction(
        commandId = Some(UUID.randomUUID().toString),
        transactionId = txUuid.getOrElse(UUID.randomUUID.toString),
        applicationId = Some(defaultAppId),
        actAs = List(party),
        workflowId = Some(defaultWorkflowId),
        ledgerEffectiveTime = Instant.now,
        recordedAt = Instant.now,
        transaction = txBuilder.buildCommitted(),
        explicitDisclosure = Map(createNodeId -> Set(party)),
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
        chosenValue = LfValue.ValueUnit,
        stakeholders = Set(party),
        signatories = Set(party),
        choiceObservers = Set.empty,
        children = ImmArray.empty,
        exerciseResult = Some(LfValue.ValueUnit),
        key = maybeKey.map(k => KeyWithMaintainers(someContractKey(party, k), Set(party))),
        byKey = false,
        version = TransactionVersion.minVersion,
      )
    )
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some(defaultAppId),
      actAs = List(party),
      workflowId = Some(defaultWorkflowId),
      ledgerEffectiveTime = Instant.now,
      recordedAt = Instant.now,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(archiveNodeId -> Set(party)),
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
        KeyWithMaintainers(someContractKey(party, key), Set(party)),
        result,
        version = TransactionVersion.minVersion,
      )
    )
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some(defaultAppId),
      actAs = List(party),
      workflowId = Some(defaultWorkflowId),
      ledgerEffectiveTime = Instant.now(),
      recordedAt = Instant.now(),
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(lookupByKeyNodeId -> Set(party)),
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
        version = TransactionVersion.minVersion,
      )
    )
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some(defaultAppId),
      actAs = List(party),
      workflowId = Some(defaultWorkflowId),
      ledgerEffectiveTime = Instant.now(),
      recordedAt = Instant.now(),
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(fetchNodeId -> Set(party)),
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

  def nextOffsetStep(offset: Offset): OffsetStep =
    OffsetStep(previousOffset.getAndSet(Some(offset)), offset)
}

object JdbcLedgerDaoSuite {

  import scalaz.syntax.traverse._
  import scalaz.{Free, Monad, NaturalTransformation, Traverse}

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
