// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.{DarParser, Decode}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{Identifier, PackageName, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{FrontStack, ImmArray, Ref, Time}
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.*
import com.daml.lf.transaction.test.{NodeIdTransactionBuilder, TransactionBuilder}
import com.daml.lf.value.Value.{ContractId, ContractInstance, ValueText, VersionedContractInstance}
import com.daml.lf.value.Value as LfValue
import com.digitalasset.canton.ledger.api.domain.TemplateFilter
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.entries.LedgerEntry
import com.digitalasset.canton.testing.utils.{TestModels, TestResourceUtils}
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.{AsyncTestSuite, OptionValues}

import java.util.UUID
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.chaining.*

private[dao] trait JdbcLedgerDaoSuite extends JdbcLedgerDaoBackend with OptionValues {
  this: AsyncTestSuite =>

  protected implicit final val loggingContext: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

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

  private[this] val dar =
    TestModels.com_daml_ledger_test_ModelTestDar_path
      .pipe(TestResourceUtils.resourceFileFromJar)
      .pipe(DarParser.assertReadArchiveFromFile(_))

  private val now = Timestamp.now()

  protected final val packages: List[(DamlLf.Archive, v2.PackageDetails)] =
    dar.all.map(dar => dar -> v2.PackageDetails(dar.getSerializedSize.toLong, now, None))
  private val testPackageId: Ref.PackageId = Ref.PackageId.assertFromString(dar.main.getHash)
  protected val testLanguageVersion: LanguageVersion =
    Decode.assertDecodeArchive(dar.main)._2.languageVersion

  protected final val alice = Party.assertFromString("Alice")
  protected final val bob = Party.assertFromString("Bob")
  protected final val charlie = Party.assertFromString("Charlie")
  protected final val david = Party.assertFromString("David")
  protected final val emma = Party.assertFromString("Emma")

  protected final val defaultAppId = "default-app-id"
  protected final val defaultWorkflowId = "default-workflow-id"

  // Note: *identifiers* and *values* defined below MUST correspond to //test-common/src/main/daml/model/Test.daml
  // This is because some tests request values in verbose mode, which requires filling in missing type information,
  // which in turn requires loading Daml-LF packages with valid Daml-LF types that correspond to the Daml-LF values.
  //
  // On the other hand, *transactions* do not need to correspond to valid transactions that could be produced by the
  // above mentioned Daml code, e.g., signatories/stakeholders may not correspond to the contract/choice arguments.
  // This is because JdbcLedgerDao is only concerned with serialization, and does not verify the Daml ledger model.
  private def testIdentifier(name: String) = Identifier(
    testPackageId,
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString("Test"),
      Ref.DottedName.assertFromString(name),
    ),
  )

  protected final val someTemplateId = testIdentifier("ParameterShowcase")
  protected final val somePackageName = PackageName.assertFromString("pkg-name")
  protected final val someTemplateIdFilter =
    TemplateFilter(someTemplateId, includeCreatedEventBlob = false)
  protected final val someValueText = LfValue.ValueText("some text")
  protected final val someValueInt = LfValue.ValueInt64(1)
  protected final val someValueNumeric =
    LfValue.ValueNumeric(com.daml.lf.data.Numeric.assertFromString("1.1"))
  protected final val someNestedOptionalInteger = LfValue.ValueRecord(
    None,
    ImmArray(
      None -> LfValue.ValueVariant(
        None,
        Ref.Name.assertFromString("SomeInteger"),
        someValueInt,
      )
    ),
  )
  protected final val someContractArgument = LfValue.ValueRecord(
    None,
    ImmArray(
      None -> LfValue.ValueParty(alice),
      None -> someValueInt,
      None -> someValueNumeric,
      None -> someValueText,
      None -> LfValue.ValueBool(true),
      None -> LfValue.ValueTimestamp(Time.Timestamp.Epoch),
      None -> someNestedOptionalInteger,
      None -> LfValue.ValueList(FrontStack(someValueInt)),
      None -> LfValue.ValueOptional(Some(someValueText)),
    ),
  )
  protected final val someChoiceName = Ref.Name.assertFromString("Choice1")
  protected final val someChoiceArgument = LfValue.ValueRecord(
    Some(testIdentifier(someChoiceName)),
    ImmArray(
      None -> someValueInt,
      None -> someValueNumeric,
      None -> someValueText,
      None -> LfValue.ValueBool(true),
      None -> LfValue.ValueTimestamp(Time.Timestamp.Epoch),
      None -> someNestedOptionalInteger,
      None -> LfValue.ValueList(FrontStack(someValueInt)),
      None -> LfValue.ValueOptional(Some(someValueText)),
    ),
  )
  protected final val someChoiceResult =
    LfValue.ValueContractId(ContractId.V1(Hash.hashPrivateKey("#1")))

  protected final def someContractKey(party: Party, value: String): LfValue.ValueRecord =
    LfValue.ValueRecord(
      None,
      ImmArray(
        None -> LfValue.ValueParty(party),
        None -> LfValue.ValueText(value),
      ),
    )

  private[this] val txVersion = TransactionVersion.StableVersions.min
  private[this] def newBuilder(): NodeIdTransactionBuilder = new NodeIdTransactionBuilder

  protected final val someContractInstance =
    ContractInstance(
      packageName = somePackageName,
      template = someTemplateId,
      arg = someContractArgument,
    )
  protected final val someVersionedContractInstance = Versioned(txVersion, someContractInstance)

  protected final val otherTemplateId = testIdentifier("Dummy")
  protected final val otherTemplateIdFilter =
    TemplateFilter(otherTemplateId, includeCreatedEventBlob = false)
  protected final val otherContractArgument = LfValue.ValueRecord(
    None,
    ImmArray(None -> LfValue.ValueParty(alice)),
  )

  private[dao] def store(
      completionInfo: Option[state.CompletionInfo],
      tx: LedgerEntry.Transaction,
      offset: Offset,
      blindingInfo: Option[BlindingInfo],
  ): Future[(Offset, LedgerEntry.Transaction)] =
    for {
      _ <- ledgerDao.storeTransaction(
        completionInfo = completionInfo,
        workflowId = tx.workflowId,
        transactionId = tx.transactionId,
        ledgerEffectiveTime = tx.ledgerEffectiveTime,
        offset = offset,
        transaction = tx.transaction,
        blindingInfo = blindingInfo,
        hostedWitnesses = Nil,
        recordTime = tx.recordedAt,
      )
    } yield offset -> tx

  protected implicit def toParty(s: String): Party = Party.assertFromString(s)

  protected implicit def toLedgerString(s: String): Ref.LedgerString =
    Ref.LedgerString.assertFromString(s)

  implicit def toApplicationId(s: String): Ref.ApplicationId =
    Ref.ApplicationId.assertFromString(s)

  protected final def create(
      absCid: ContractId,
      signatories: Set[Party] = Set(alice, bob),
      templateId: Identifier = someTemplateId,
      contractArgument: LfValue = someContractArgument,
      observers: Set[Party] = Set.empty,
  ): Node.Create =
    createNode(absCid, signatories, signatories ++ observers, None, templateId, contractArgument)

  protected final def createNode(
      absCid: ContractId,
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[GlobalKeyWithMaintainers] = None,
      templateId: Identifier = someTemplateId,
      contractArgument: LfValue = someContractArgument,
  ): Node.Create =
    Node.Create(
      coid = absCid,
      templateId = templateId,
      packageName = somePackageName,
      arg = contractArgument,
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = key,
      version = txVersion,
    )

  protected final def exerciseNode(
      targetCid: ContractId,
      key: Option[GlobalKeyWithMaintainers] = None,
  ): Node.Exercise =
    Node.Exercise(
      targetCoid = targetCid,
      templateId = someTemplateId,
      packageName = somePackageName,
      interfaceId = None,
      choiceId = someChoiceName,
      consuming = true,
      actingParties = Set(alice),
      chosenValue = someChoiceArgument,
      stakeholders = Set(alice, bob),
      signatories = Set(alice, bob),
      choiceObservers = Set.empty,
      choiceAuthorizers = None,
      children = ImmArray.Empty,
      exerciseResult = Some(someChoiceResult),
      keyOpt = key,
      byKey = false,
      version = txVersion,
    )

  protected final def fetchNode(
      contractId: ContractId,
      party: Party = alice,
  ): Node.Fetch =
    Node.Fetch(
      coid = contractId,
      templateId = someTemplateId,
      packageName = somePackageName,
      actingParties = Set(party),
      signatories = Set(party),
      stakeholders = Set(party),
      keyOpt = None,
      byKey = false,
      version = txVersion,
    )

  // Ids of all contracts created in a transaction - both transient and non-transient
  protected def created(tx: LedgerEntry.Transaction): Set[ContractId] =
    tx.transaction.fold(Set.empty[ContractId]) {
      case (set, (_, create: Node.Create)) =>
        set + create.coid
      case (set, _) =>
        set
    }

  // All non-transient contracts created in a transaction
  protected def nonTransient(tx: LedgerEntry.Transaction): Set[ContractId] =
    tx.transaction.fold(Set.empty[ContractId]) {
      case (set, (_, create: Node.Create)) =>
        set + create.coid
      case (set, (_, exercise: Node.Exercise)) if exercise.consuming =>
        set - exercise.targetCoid
      case (set, _) =>
        set
    }

  protected final def singleCreate: (Offset, LedgerEntry.Transaction) =
    singleCreate(create(_))

  protected final def multiPartySingleCreate: (Offset, LedgerEntry.Transaction) =
    singleCreate(create(_, Set(alice, bob)), List(alice, bob))

  protected final def singleCreate(
      create: ContractId => Node.Create,
      actAs: List[Party] = List(alice),
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = newBuilder()
    val cid = txBuilder.newCid
    val creation = create(cid)
    val eid = txBuilder.add(creation)
    val offset = nextOffset()
    val id = offset.toLong
    val let = Timestamp.now()
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"commandId$id"),
      transactionId = s"trId$id",
      applicationId = Some("appID1"),
      submissionId = Some(s"submissionId$id"),
      actAs = actAs,
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(eid -> (creation.signatories union creation.stakeholders)),
    )
  }

  protected final def noSubmitterInfo(
      transaction: LedgerEntry.Transaction
  ): LedgerEntry.Transaction =
    transaction.copy(commandId = None, actAs = List.empty, applicationId = None)

  protected final def fromTransaction(
      transaction: CommittedTransaction,
      actAs: List[Party] = List(alice),
  ): (Offset, LedgerEntry.Transaction) = {
    val offset = nextOffset()
    val id = offset.toLong
    val let = Timestamp.now()
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"commandId$id"),
      transactionId = s"trId$id",
      applicationId = Some("appID1"),
      submissionId = Some(s"submissionId$id"),
      actAs = actAs,
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = transaction,
      explicitDisclosure = Map.empty,
    )
  }

  protected final def createTestKey(
      maintainers: Set[Party]
  ): GlobalKeyWithMaintainers = {
    val aTextValue = ValueText(scala.util.Random.nextString(10))
    GlobalKeyWithMaintainers.assertBuild(someTemplateId, aTextValue, maintainers)
  }

  protected final def createAndStoreContract(
      submittingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[GlobalKeyWithMaintainers],
      contractArgument: LfValue = someContractArgument,
  ): Future[(Offset, LedgerEntry.Transaction)] =
    store(
      singleCreate(
        create = { cid =>
          createNode(
            absCid = cid,
            signatories = signatories,
            stakeholders = stakeholders,
            key = key,
            contractArgument = contractArgument,
          )
        },
        actAs = submittingParties.toList,
      )
    )

  protected def singleExercise(
      targetCid: ContractId,
      key: Option[GlobalKeyWithMaintainers] = None,
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = newBuilder()
    val nid = txBuilder.add(exerciseNode(targetCid, key))
    val offset = nextOffset()
    val id = offset.toLong
    val let = Timestamp.now()
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"commandId$id"),
      transactionId = s"trId$id",
      applicationId = Some("appID1"),
      submissionId = Some(s"submissionId$id"),
      actAs = List("Alice"),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(nid -> Set("Alice", "Bob")),
    )
  }

  protected def multiPartySingleExercise(
      targetCid: ContractId
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = newBuilder()
    val nid = txBuilder.add(exerciseNode(targetCid))
    val offset = nextOffset()
    val id = offset.toLong
    val let = Timestamp.now()
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"commandId$id"),
      transactionId = s"trId$id",
      applicationId = Some("appID1"),
      submissionId = Some(s"submissionId$id"),
      actAs = List(alice, bob, charlie),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(nid -> Set(alice, bob)),
    )
  }

  protected def singleNonConsumingExercise(
      targetCid: ContractId
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = newBuilder()
    val nid = txBuilder.add(exerciseNode(targetCid).copy(consuming = false))
    val offset = nextOffset()
    val id = offset.toLong
    val let = Timestamp.now()
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"commandId$id"),
      transactionId = s"trId$id",
      applicationId = Some("appID1"),
      submissionId = Some(s"submissionId$id"),
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
    val txBuilder = newBuilder()
    val exerciseId = txBuilder.add(exerciseNode(targetCid))
    val childId = txBuilder.add(create(txBuilder.newCid), exerciseId)
    val tx = txBuilder.buildCommitted()
    val offset = nextOffset()
    val id = offset.toLong
    val txId = s"trId$id"
    val let = Timestamp.now()
    offset -> LedgerEntry.Transaction(
      commandId = Some(s"commandId$id"),
      transactionId = txId,
      applicationId = Some("appID1"),
      submissionId = Some(s"submissionId$id"),
      actAs = List("Alice"),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = let,
      recordedAt = let,
      transaction = tx,
      explicitDisclosure = Map(exerciseId -> Set("Alice", "Bob"), childId -> Set("Alice", "Bob")),
    )
  }

  protected def fullyTransient(
      create: ContractId => Node.Create = create(_)
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = newBuilder()
    val cid = txBuilder.newCid
    val createId = txBuilder.add(create(cid))
    val exerciseId = txBuilder.add(exerciseNode(cid))
    val let = Timestamp.now()
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID().toString,
      applicationId = Some("appID1"),
      submissionId = Some(UUID.randomUUID.toString),
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
    val txBuilder = newBuilder()
    val root = txBuilder.newCid
    val transient = txBuilder.newCid
    val rootCreateId = txBuilder.add(create(root))
    val rootExerciseId = txBuilder.add(exerciseNode(root).copy(actingParties = Set(charlie)))
    val createTransientId = txBuilder.add(create(transient), rootExerciseId)
    val consumeTransientId = txBuilder.add(exerciseNode(transient), rootExerciseId)
    val let = Timestamp.now()
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID.toString),
      transactionId = UUID.randomUUID().toString,
      applicationId = Some("appID1"),
      submissionId = Some(UUID.randomUUID.toString),
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
    val txBuilder = newBuilder()
    val createId = txBuilder.add(
      create(txBuilder.newCid).copy(
        signatories = Set(charlie),
        stakeholders = Set(charlie),
      )
    )
    val exerciseId = txBuilder.add(
      exerciseNode(txBuilder.newCid).copy(
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
    val let = Timestamp.now()
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID().toString,
      applicationId = Some("appID1"),
      submissionId = Some(UUID.randomUUID().toString),
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
      signatoriesAndTemplates: Seq[(Party, Identifier, LfValue)],
  ): (Offset, LedgerEntry.Transaction) = {
    require(signatoriesAndTemplates.nonEmpty, "multipleCreates cannot create empty transactions")
    val txBuilder = newBuilder()
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
      submissionId = Some(UUID.randomUUID.toString),
      actAs = List(operator),
      workflowId = Some("workflowId"),
      ledgerEffectiveTime = Timestamp.now(),
      recordedAt = Timestamp.now(),
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = disclosure.toMap,
    )
  }

  protected final def store(
      blindingInfo: Option[BlindingInfo],
      offsetAndTx: (Offset, LedgerEntry.Transaction),
  ): Future[(Offset, LedgerEntry.Transaction)] = {
    val (offset, entry) = offsetAndTx
    val info = completionInfoFrom(entry)
    store(info, entry, offset, blindingInfo)
  }

  protected def completionInfoFrom(entry: LedgerEntry.Transaction): Option[state.CompletionInfo] =
    for {
      actAs <- if (entry.actAs.isEmpty) None else Some(entry.actAs)
      applicationId <- entry.applicationId
      commandId <- entry.commandId
      submissionId <- entry.submissionId
    } yield state.CompletionInfo(
      actAs,
      applicationId,
      commandId,
      None,
      Some(submissionId),
      Some(TransactionNodeStatistics(entry.transaction)),
    )

  protected final def store(
      offsetAndTx: (Offset, LedgerEntry.Transaction)
  ): Future[(Offset, LedgerEntry.Transaction)] =
    store(
      blindingInfo = None,
      offsetAndTx = offsetAndTx,
    )

  protected final def storeSync(
      commands: Vector[(Offset, LedgerEntry.Transaction)]
  ): Future[Vector[(Offset, LedgerEntry.Transaction)]] = {

    import com.daml.scalautil.TraverseFMSyntax.*
    import scalaz.std.scalaFuture.*
    import scalaz.std.vector.*

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
    val txBuilder = newBuilder()
    val createNodeId = txBuilder.add(
      Node.Create(
        coid = txBuilder.newCid,
        templateId = someTemplateId,
        packageName = somePackageName,
        arg = someContractArgument,
        signatories = Set(party),
        stakeholders = Set(party),
        keyOpt = Some(
          GlobalKeyWithMaintainers
            .assertBuild(someTemplateId, someContractKey(party, key), Set(party))
        ),
        version = txVersion,
      )
    )
    nextOffset() ->
      LedgerEntry.Transaction(
        commandId = Some(UUID.randomUUID().toString),
        transactionId = txUuid.getOrElse(UUID.randomUUID.toString),
        applicationId = Some(defaultAppId),
        submissionId = Some(UUID.randomUUID().toString),
        actAs = List(party),
        workflowId = Some(defaultWorkflowId),
        ledgerEffectiveTime = Timestamp.now(),
        recordedAt = Timestamp.now(),
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
    val txBuilder = newBuilder()
    val archiveNodeId = txBuilder.add(
      Node.Exercise(
        targetCoid = contractId,
        templateId = someTemplateId,
        packageName = somePackageName,
        interfaceId = None,
        choiceId = Ref.ChoiceName.assertFromString("Archive"),
        consuming = true,
        actingParties = Set(party),
        chosenValue = LfValue.ValueUnit,
        stakeholders = Set(party),
        signatories = Set(party),
        choiceObservers = Set.empty,
        choiceAuthorizers = None,
        children = ImmArray.Empty,
        exerciseResult = Some(LfValue.ValueUnit),
        keyOpt = maybeKey.map(k =>
          GlobalKeyWithMaintainers
            .assertBuild(someTemplateId, someContractKey(party, k), Set(party))
        ),
        byKey = false,
        version = txVersion,
      )
    )
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some(defaultAppId),
      submissionId = Some(UUID.randomUUID().toString),
      actAs = List(party),
      workflowId = Some(defaultWorkflowId),
      ledgerEffectiveTime = Timestamp.now(),
      recordedAt = Timestamp.now(),
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
    val txBuilder = newBuilder()
    val lookupByKeyNodeId = txBuilder.add(
      Node.LookupByKey(
        templateId = someTemplateId,
        packageName = somePackageName,
        key = GlobalKeyWithMaintainers
          .assertBuild(someTemplateId, someContractKey(party, key), Set(party)),
        result = result,
        version = txVersion,
      )
    )
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some(defaultAppId),
      submissionId = Some(UUID.randomUUID().toString),
      actAs = List(party),
      workflowId = Some(defaultWorkflowId),
      ledgerEffectiveTime = Timestamp.now(),
      recordedAt = Timestamp.now(),
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(lookupByKeyNodeId -> Set(party)),
    )
  }

  protected final def txFetch(
      party: Party,
      contractId: ContractId,
  ): (Offset, LedgerEntry.Transaction) = {
    val txBuilder = newBuilder()
    val fetchNodeId = txBuilder.add(
      Node.Fetch(
        coid = contractId,
        templateId = someTemplateId,
        packageName = somePackageName,
        actingParties = Set(party),
        signatories = Set(party),
        stakeholders = Set(party),
        keyOpt = None,
        byKey = false,
        version = txVersion,
      )
    )
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some(defaultAppId),
      submissionId = Some(UUID.randomUUID().toString),
      actAs = List(party),
      workflowId = Some(defaultWorkflowId),
      ledgerEffectiveTime = Timestamp.now(),
      recordedAt = Timestamp.now(),
      transaction = txBuilder.buildCommitted(),
      explicitDisclosure = Map(fetchNodeId -> Set(party)),
    )
  }

  protected final def emptyTransaction(party: Party): (Offset, LedgerEntry.Transaction) =
    nextOffset() -> LedgerEntry.Transaction(
      commandId = Some(UUID.randomUUID().toString),
      transactionId = UUID.randomUUID.toString,
      applicationId = Some(defaultAppId),
      submissionId = Some(UUID.randomUUID().toString),
      actAs = List(party),
      workflowId = Some(defaultWorkflowId),
      ledgerEffectiveTime = Timestamp.now(),
      recordedAt = Timestamp.now(),
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
      .map(_._2.completion.toList.head)
      .map(c => c.commandId -> c.status.value.code)
      .runWith(Sink.seq)

}

object JdbcLedgerDaoSuite {

  private type DivulgedContracts =
    Map[(ContractId, VersionedContractInstance), Set[Party]]

}
