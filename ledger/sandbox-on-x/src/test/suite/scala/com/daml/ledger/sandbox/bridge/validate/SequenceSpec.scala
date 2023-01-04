// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import java.time.Duration
import com.daml.api.util.TimeProvider
import com.daml.error.ErrorCode
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.utils.ErrorDetails
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update.CommandRejected
import com.daml.ledger.participant.state.v2.Update.CommandRejected.RejectionReasonTemplate
import com.daml.ledger.participant.state.v2.{CompletionInfo, SubmitterInfo, TransactionMeta, Update}
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.bridge.LedgerBridge.toOffset
import com.daml.ledger.sandbox.bridge.validate.ConflictCheckingLedgerBridge.Validation
import com.daml.ledger.sandbox.domain.{Rejection, Submission}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.IdString
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.daml.lf.transaction.Transaction.{KeyActive, KeyCreate}
import com.daml.lf.transaction._
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.rpc.status.Status.toJavaProto
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, FixtureContext, OptionValues}

import scala.jdk.CollectionConverters.CollectionHasAsScala

class SequenceSpec
    extends AnyFlatSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with OptionValues {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  behavior of classOf[SequenceImpl].getSimpleName

  it should "validate party upload submission" in new TestContext {
    // Assert successful party allocation
    private val partyUploadInput = input(partyAllocationSubmission)

    sequence(partyUploadInput) shouldBe Iterable(
      toOffset(1L) -> Update.PartyAddedToParticipant(
        party = allocatedParty,
        displayName = displayName,
        participantId = Ref.ParticipantId.assertFromString(participantName),
        recordTime = currentRecordTime,
        submissionId = Some(submissionId),
      )
    )

    // Assert duplicate party allocation is rejected
    sequence(partyUploadInput) shouldBe Iterable(
      toOffset(2L) -> Update.PartyAllocationRejected(
        submissionId = submissionId,
        participantId = Ref.ParticipantId.assertFromString(participantName),
        recordTime = currentRecordTime,
        rejectionReason = "Party already exists",
      )
    )
  }

  it should "validate configuration upload submission" in new TestContext {
    // Assert config upload
    private val configUploadInput = input(NoOpPreparedSubmission(configUpload))
    sequence(configUploadInput) shouldBe Iterable(
      toOffset(1L) -> Update.ConfigurationChanged(
        recordTime = currentRecordTime,
        submissionId = submissionId,
        participantId = Ref.ParticipantId.assertFromString(participantName),
        newConfiguration = config,
      )
    )

    // Assert config state is set
    sequenceImpl.ledgerConfiguration shouldBe Some(config)

    private val config2: Configuration =
      config.copy(maxDeduplicationDuration = config.maxDeduplicationDuration.plusSeconds(60L))
    private val configUploadInput2 =
      input(NoOpPreparedSubmission(configUpload.copy(config = config2)))

    // Assert rejection on expected generation mismatch (duplicate submission)
    sequence(configUploadInput2) shouldBe Iterable(
      toOffset(2L) -> Update.ConfigurationChangeRejected(
        recordTime = currentRecordTime,
        submissionId = submissionId,
        participantId = Ref.ParticipantId.assertFromString(participantName),
        proposedConfiguration = config2,
        rejectionReason = s"Generation mismatch: expected=Some(2), actual=1",
      )
    )

    // Advance record time past maxRecordTime
    private val currentTime2 = maxConfigRecordTime.addMicros(1000L)
    when(timeProviderMock.getCurrentTimestamp).thenReturn(currentTime2)

    // Set correct generation but leave old maxRecordTime
    private val config3 = config.copy(generation = 2L)
    private val configUploadInput3 =
      input(NoOpPreparedSubmission(configUpload.copy(config = config3)))

    // Assert rejection on configuration change time-out
    sequence(configUploadInput3) shouldBe Iterable(
      toOffset(3L) -> Update.ConfigurationChangeRejected(
        recordTime = currentTime2,
        submissionId = submissionId,
        participantId = Ref.ParticipantId.assertFromString(participantName),
        proposedConfiguration = config3,
        rejectionReason = s"Configuration change timed out: $currentTime2 > $maxConfigRecordTime",
      )
    )

    // Assert config state is unchanged after rejection
    sequenceImpl.ledgerConfiguration shouldBe Some(config)
  }

  it should "fail on time model check" in new TestContext {
    val Seq((offset, update)) = sequence(input(lateSubmission))
    offset shouldBe toOffset(1L)

    assertCommandRejected(
      update = update,
      errorCode = LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime,
      reason =
        "Ledger time 1970-01-01T00:01:00.001Z outside of range [1969-12-31T23:59:30.001Z, 1970-01-01T00:00:30.001Z]",
    )
  }

  it should "fail on unallocated transaction informees" in new TestContext {
    val Seq((offset, update)) = sequence(input(txWithUnallocatedParty))
    offset shouldBe toOffset(1L)
    assertCommandRejected(
      update = update,
      errorCode = LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger,
      reason = "Parties not known on ledger: [new-guy]",
    )
  }

  it should "reject a transaction if there is no ledger configuration" in new TestContext {
    private val sequenceWithoutLedgerConfig = buildSequence(initialLedgerConfiguration = None)()

    // Assert transaction rejection on missing ledger configuration
    val Seq((offset, update)) = sequenceWithoutLedgerConfig(create(cId(1), Some(contractKey(1L))))
    offset shouldBe toOffset(1L)
    assertCommandRejected(
      update = update,
      errorCode = LedgerApiErrors.RequestValidation.NotFound.LedgerConfiguration,
      reason = "The ledger configuration could not be retrieved: Cannot validate ledger time",
    )

    // Upload config to ledger
    sequenceWithoutLedgerConfig(input(NoOpPreparedSubmission(configUpload))) shouldBe Iterable(
      toOffset(2L) -> Update.ConfigurationChanged(
        recordTime = currentRecordTime,
        submissionId = submissionId,
        participantId = Ref.ParticipantId.assertFromString(participantName),
        newConfiguration = config,
      )
    )

    // Assert transaction accepted after ledger config upload
    val Seq((offset3, update3)) = sequenceWithoutLedgerConfig(create(cId(1), Some(contractKey(1L))))
    offset3 shouldBe toOffset(3L)
    update3 shouldBe transactionAccepted(3)
  }

  it should "assert internal consistency validation on transaction submission conflicts" in new TestContext {
    // Conflict validation passes on empty Sequencer State
    val Seq((offset1, update1)) =
      sequence(create(cId(1), Some(contractKey(1L)), cmdId = commandId(2)))
    offset1 shouldBe toOffset(1L)
    update1 shouldBe transactionAccepted(1, cmdId = commandId(2))

    // Attempt assigning an active contract key
    val Seq((offset2, update2)) =
      sequence(create(cId(2), Some(contractKey(1L)), cmdId = commandId(2)))
    offset2 shouldBe toOffset(2L)
    assertCommandRejected(
      update2,
      errorCode = LedgerApiErrors.ConsistencyErrors.DuplicateContractKey,
      "contract key is not unique",
      cmdId = commandId(2),
    )

    // Archiving a contract for the first time should succeed
    val Seq((offset3, update3)) = sequence(consume(cId(3), cmdId = commandId(3)))
    offset3 shouldBe toOffset(3L)
    update3 shouldBe transactionAccepted(3, cmdId = commandId(3))

    // Reject when trying to archive a contract again
    val Seq((offset4, update4)) = sequence(consume(cId(3), cmdId = commandId(3)))
    offset4 shouldBe toOffset(4L)
    assertCommandRejected(
      update = update4,
      errorCode = LedgerApiErrors.ConsistencyErrors.ContractNotFound,
      reason = s"Unknown contracts: [${cId(3).coid}]",
      cmdId = commandId(3),
    )

    // Archiving a contract with an assigned key for the first time succeeds
    val Seq((offset5, update5)) =
      sequence(consume(cId(4), Some(contractKey(2L)), cmdId = commandId(4)))
    offset5 shouldBe toOffset(5L)
    update5 shouldBe transactionAccepted(5, cmdId = commandId(4))

    // Reject on unknown key
    val Seq((offset6, update6)) =
      sequence(exerciseNonConsuming(cId(5), contractKey(2L), cmdId = commandId(5)))
    offset6 shouldBe toOffset(6L)
    assertCommandRejected(
      update = update6,
      errorCode = LedgerApiErrors.ConsistencyErrors.InconsistentContractKey,
      reason =
        s"Contract key lookup with different results: expected [None], actual [Some(${cId(5)})]",
      cmdId = commandId(5),
    )

    // Reject on inconsistent key usage
    val Seq((offset7, update7)) =
      sequence(exerciseNonConsuming(cId(5), contractKey(1L), cmdId = commandId(5)))
    offset7 shouldBe toOffset(7L)
    assertCommandRejected(
      update = update7,
      errorCode = LedgerApiErrors.ConsistencyErrors.InconsistentContractKey,
      reason =
        s"Contract key lookup with different results: expected [Some(${cId(1)})], actual [Some(${cId(5)})]",
      cmdId = commandId(5),
    )
  }

  it should "forward the noConflictUpTo offsets to the sequencer state queue and allow its pruning" in new TestContext {
    // Ingest two transactions which are archiving contracts
    val Seq((offset1, _)) =
      sequence(consume(contractId = cId(1), noConflictUpTo = toOffset(0L), cmdId = commandId(1)))
    val Seq((offset2, _)) =
      sequence(consume(contractId = cId(2), noConflictUpTo = toOffset(0L), cmdId = commandId(2)))

    // Check that the sequencer queue includes the updates
    sequenceImpl.sequencerState.sequencerQueue should contain theSameElementsAs Vector(
      offset1 -> (Map.empty, Set(cId(1))),
      offset2 -> (Map.empty, Set(cId(2))),
    )
    sequenceImpl.sequencerState.consumedContractsState shouldBe Set(cId(1), cId(2))

    // Ingest another transaction with the noConflictUpTo equal to the offset of the previous transaction
    val Seq((offset3, _)) =
      sequence(consume(contractId = cId(3), noConflictUpTo = offset2, cmdId = commandId(3)))

    // Assert that the queue has pruned the previous entries
    sequenceImpl.sequencerState.sequencerQueue should contain theSameElementsAs Vector(
      offset3 -> (Map.empty, Set(cId(3)))
    )
    sequenceImpl.sequencerState.consumedContractsState shouldBe Set(cId(3))
  }

  it should "convert an incoming domain rejection to a CommandRejected update" in new TestContext {
    sequence(Left(rejectionMock)) shouldBe Iterable(toOffset(1L) -> commandRejectedUpdateMock)
  }

  it should "deduplicate commands" in new TestContext {
    // Command with non-zero deduplication period
    private val initialSubmission = create(cId(1))

    private val deduplicationPeriod: DeduplicationPeriod.DeduplicationDuration =
      DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(1L))

    private val submissionWithDedupPeriod = create(
      cId(1),
      transactionSubmission =
        tx.copy(submitterInfo = tx.submitterInfo.copy(deduplicationPeriod = deduplicationPeriod)),
      cmdId = commandId(1),
    )

    val Seq((offset1, update1)) = sequence(initialSubmission)
    offset1 shouldBe toOffset(1L)
    update1 shouldBe transactionAccepted(1, cmdId = commandId(1))

    // Assert duplicate command rejected
    val Seq((offset2, update2)) = sequence(submissionWithDedupPeriod)
    offset2 shouldBe toOffset(2L)
    assertCommandRejected(
      update = update2,
      errorCode = LedgerApiErrors.ConsistencyErrors.DuplicateCommand,
      reason = "A command with the given command id has already been successfully processed",
      deduplicationPeriod = deduplicationPeriod,
      cmdId = commandId(1),
    )

    // Advance record time past the deduplication period
    private val newRecordTime: Timestamp =
      currentRecordTime.add(deduplicationPeriod.duration.plusSeconds(1L))
    when(timeProviderMock.getCurrentTimestamp).thenReturn(newRecordTime)

    // Assert command is accepted
    val Seq((offset3, update3)) = sequence(submissionWithDedupPeriod)
    offset3 shouldBe toOffset(3L)
    update3 shouldBe transactionAccepted(
      txId = 3,
      completionInfo = completionInfo.copy(optDeduplicationPeriod = Some(deduplicationPeriod)),
      recordTime = newRecordTime,
    )
  }

  private trait TestContext extends FixtureContext {
    private val bridgeMetrics = new BridgeMetrics(Metrics.ForTesting)
    val timeProviderMock: TimeProvider = mock[TimeProvider]
    val submissionId: IdString.LedgerString =
      Ref.SubmissionId.assertFromString("some-submission-id")
    val participantName: String = "participant"
    val allocatedInformees: Set[IdString.Party] =
      (1 to 3).map(idx => s"party-$idx").map(Ref.Party.assertFromString).toSet

    private val initialLedgerConfiguration: Some[Configuration] = Some(
      Configuration(
        generation = 0L,
        timeModel = LedgerTimeModel.reasonableDefault,
        maxDeduplicationDuration = Duration.ofSeconds(60L),
      )
    )

    val maxDeduplicationDuration: Duration = Duration.ofDays(1L)

    val sequenceImpl: SequenceImpl = buildSequence()
    val sequence: Validation[(Offset, PreparedSubmission)] => Iterable[(Offset, Update)] =
      sequenceImpl()

    val currentRecordTime: Time.Timestamp = Time.Timestamp.assertFromLong(1000L)
    when(timeProviderMock.getCurrentTimestamp).thenReturn(currentRecordTime)

    private val zeroDeduplicationPeriod: DeduplicationPeriod.DeduplicationDuration =
      DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(0L))

    // Transaction submission mocks
    val submitterInfo: SubmitterInfo = SubmitterInfo(
      actAs = List.empty,
      readAs = List.empty,
      applicationId = Ref.ApplicationId.assertFromString("applicationId"),
      commandId = commandId(1),
      deduplicationPeriod = zeroDeduplicationPeriod,
      submissionId = Some(submissionId),
      ledgerConfiguration =
        Configuration(0L, LedgerTimeModel.reasonableDefault, Duration.ofSeconds(0L)),
    )
    val txLedgerEffectiveTime: Timestamp = currentRecordTime
    val transactionMeta: TransactionMeta = TransactionMeta(
      txLedgerEffectiveTime,
      None,
      Time.Timestamp.Epoch,
      Hash.hashPrivateKey("a key"),
      None,
      None,
      None,
    )
    val txMock: SubmittedTransaction = TransactionBuilder.EmptySubmitted

    val completionInfo: CompletionInfo =
      submitterInfo.toCompletionInfo(Some(TransactionNodeStatistics(txMock)))

    val tx: Submission.Transaction = Submission.Transaction(
      submitterInfo = submitterInfo,
      transactionMeta = transactionMeta,
      transaction = txMock,
      estimatedInterpretationCost = 0L,
      disclosedContracts = ImmArray.empty,
    )(loggingContext)

    val txInformees: Set[IdString.Party] = allocatedInformees.take(2)
    val defaultTxSubmission: PreparedTransactionSubmission =
      PreparedTransactionSubmission(
        keyInputs = Map.empty,
        inputContracts = Set.empty,
        updatedKeys = Map.empty,
        consumedContracts = Set.empty,
        blindingInfo = BlindingInfo(Map.empty, Map.empty),
        transactionInformees = txInformees,
        submission = tx,
      )

    val lateSubmission: PreparedTransactionSubmission =
      defaultTxSubmission.copy(
        submission = tx.copy(transactionMeta =
          transactionMeta.copy(ledgerEffectiveTime =
            txLedgerEffectiveTime.add(Duration.ofSeconds(60))
          )
        )
      )
    val txWithUnallocatedParty: PreparedTransactionSubmission =
      defaultTxSubmission.copy(transactionInformees =
        txInformees + Ref.Party.assertFromString("new-guy")
      )

    // Configuration upload mocks
    val maxConfigRecordTime: Timestamp = currentRecordTime.addMicros(1000L)
    val config: Configuration = Configuration(
      generation = 1L,
      timeModel = LedgerTimeModel.reasonableDefault,
      maxDeduplicationDuration = Duration.ofSeconds(0L),
    )
    val configUpload: Submission.Config = Submission.Config(
      maxRecordTime = maxConfigRecordTime,
      submissionId = submissionId,
      config = config,
    )

    // Party allocation mocks
    val partyHint = "some-party"
    val displayName = "Some display name"
    val allocatedParty: IdString.Party = Ref.Party.assertFromString(partyHint)
    val partyAllocationSubmission: NoOpPreparedSubmission = NoOpPreparedSubmission(
      Submission.AllocateParty(Some(allocatedParty), Some(displayName), submissionId)
    )

    // Rejection conversion mocks
    val rejectionMock: Rejection = mock[Rejection]
    val commandRejectedUpdateMock: CommandRejected =
      CommandRejected(currentRecordTime, completionInfo, mock[RejectionReasonTemplate])
    when(rejectionMock.toCommandRejectedUpdate(currentRecordTime))
      .thenReturn(commandRejectedUpdateMock)

    def create(
        contractId: ContractId,
        keyO: Option[GlobalKey] = None,
        informees: Set[Ref.Party] = txInformees,
        transactionSubmission: Submission.Transaction = tx,
        cmdId: Ref.CommandId = commandId(1),
    ): Right[Nothing, (Offset, PreparedTransactionSubmission)] = {
      val keyInputs = keyO.map(k => Map(k -> KeyCreate)).getOrElse(Map.empty)
      val updatedKeys = keyO.map(k => Map(k -> Some(contractId))).getOrElse(Map.empty)

      val preparedTransactionSubmission =
        defaultTxSubmission.copy(
          keyInputs = keyInputs,
          updatedKeys = updatedKeys,
          transactionInformees = informees,
          submission = transactionSubmission.copy(submitterInfo =
            transactionSubmission.submitterInfo.copy(commandId = cmdId)
          ),
        )

      input(preparedTransactionSubmission)
    }

    def consume(
        contractId: ContractId,
        keyO: Option[GlobalKey] = None,
        noConflictUpTo: Offset = Offset.beforeBegin,
        informees: Set[Ref.Party] = txInformees,
        transactionSubmission: Submission.Transaction = tx,
        cmdId: Ref.CommandId = commandId(1),
    ): Right[Nothing, (Offset, PreparedTransactionSubmission)] = {
      val keyInputs = keyO.map(k => Map(k -> KeyActive(contractId))).getOrElse(Map.empty)
      val updatedKeys = keyO.map(k => Map(k -> None)).getOrElse(Map.empty)
      val inputContracts = Set(contractId)

      val preparedTransactionSubmission: PreparedTransactionSubmission =
        defaultTxSubmission.copy(
          keyInputs = keyInputs,
          inputContracts = inputContracts,
          updatedKeys = updatedKeys,
          consumedContracts = inputContracts,
          transactionInformees = informees,
          submission =
            transactionSubmission.copy(submitterInfo = submitterInfo.copy(commandId = cmdId)),
        )

      Right(noConflictUpTo -> preparedTransactionSubmission)
    }

    def buildSequence(
        initialLedgerConfiguration: Option[Configuration] = initialLedgerConfiguration,
        explicitDisclosureEnabled: Boolean = false,
    ) = new SequenceImpl(
      participantId = Ref.ParticipantId.assertFromString(participantName),
      bridgeMetrics = bridgeMetrics,
      timeProvider = timeProviderMock,
      initialLedgerEnd = Offset.beforeBegin,
      initialAllocatedParties = allocatedInformees,
      initialLedgerConfiguration = initialLedgerConfiguration,
      maxDeduplicationDuration = maxDeduplicationDuration,
      explicitDisclosureEnabled = explicitDisclosureEnabled,
    )

    def exerciseNonConsuming(
        contractId: ContractId,
        key: GlobalKey,
        informees: Set[Ref.Party] = txInformees,
        transactionSubmission: Submission.Transaction = tx,
        cmdId: Ref.CommandId = commandId(1),
    ): Right[Nothing, (Offset, PreparedTransactionSubmission)] = {
      val inputContracts = Set(contractId)

      val preparedTransactionSubmission: PreparedTransactionSubmission =
        defaultTxSubmission.copy(
          keyInputs = Map(key -> KeyActive(contractId)),
          inputContracts = inputContracts,
          updatedKeys = Map.empty,
          consumedContracts = Set.empty,
          transactionInformees = informees,
          submission =
            transactionSubmission.copy(submitterInfo = submitterInfo.copy(commandId = cmdId)),
        )

      input(preparedTransactionSubmission)
    }

    def transactionAccepted(
        txId: Int,
        completionInfo: CompletionInfo = completionInfo,
        recordTime: Time.Timestamp = currentRecordTime,
        cmdId: Ref.CommandId = commandId(1),
        contractMetadata: Map[ContractId, Bytes] = Map.empty,
        tx: SubmittedTransaction = txMock,
    ): Update.TransactionAccepted =
      Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo.copy(commandId = cmdId)),
        transactionMeta = transactionMeta,
        transaction = CommittedTransaction(tx),
        transactionId = Ref.TransactionId.assertFromString(txId.toString),
        recordTime = recordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
        contractMetadata = contractMetadata,
      )

    def assertCommandRejected(
        update: Update,
        errorCode: ErrorCode,
        reason: String,
        deduplicationPeriod: DeduplicationPeriod = zeroDeduplicationPeriod,
        cmdId: Ref.CommandId = commandId(1),
    ): Assertion = update match {
      case rejection: Update.CommandRejected =>
        rejection.recordTime shouldBe currentRecordTime
        // Transaction statistics are not populated for rejections
        rejection.completionInfo shouldBe completionInfo.copy(
          statistics = None,
          optDeduplicationPeriod = Some(deduplicationPeriod),
          commandId = cmdId,
        )
        assertSelfServiceErrorCode(rejection.reasonTemplate.status, errorCode, reason)
      case noMatch => fail(s"Expectation mismatch on expected CommandRejected: $noMatch")
    }

    def input[T, V](preparedSubmission: T): Right[V, (Offset, T)] = Right(
      // noConflictUpTo is only used for pruning the sequencerState,
      // so we use the Offset.beforeBegin as mock where we don't need to assert it
      Offset.beforeBegin -> preparedSubmission
    )
  }

  private def contractKey(i: Long) = {
    val templateId = Ref.Identifier.assertFromString("pkg:M:T")
    GlobalKey(templateId, Value.ValueInt64(i))
  }

  private def cId(i: Int) = ContractId.V1(Hash.hashPrivateKey(i.toString))

  private def commandId(i: Int): Ref.CommandId = Ref.CommandId.assertFromString(s"cmd-$i")

  def assertSelfServiceErrorCode(
      scalaStatus: com.google.rpc.status.Status,
      expectedErrorCode: ErrorCode,
      errorMessageSubstring: String,
  ): Assertion = {
    val status = toJavaProto(scalaStatus)

    val actualStatusCode = status.getCode
    val actualErrorDetails = ErrorDetails.from(status.getDetailsList.asScala.toSeq)
    val actualErrorId = actualErrorDetails
      .collectFirst { case err: ErrorDetails.ErrorInfoDetail => err.errorCodeId }
      .getOrElse(fail("Actual error id is not defined"))
    val actualRetryability = actualErrorDetails
      .collectFirst { case err: ErrorDetails.RetryInfoDetail => err.duration }

    status.getMessage should include(errorMessageSubstring)
    actualErrorId shouldBe expectedErrorCode.id
    actualStatusCode shouldBe expectedErrorCode.category.grpcCode
      .map(_.value())
      .value
    actualRetryability shouldBe expectedErrorCode.category.retryable.map(_.duration)
  }
}
