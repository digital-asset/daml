// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import akka.NotUsed
import akka.stream.KillSwitches
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.{Configuration, LedgerId, LedgerInitialConditions}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{CompletionInfo, ReadService, TransactionMeta, Update}
import com.daml.lf.crypto
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{CommittedTransaction, TransactionNodeStatistics}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.google.protobuf.ByteString

import java.time.Instant

/** An infinite stream of state updates that fully conforms to the Daml ledger model.
  *
  *  All instances of this class produce the same stream of state updates.
  *
  *  @param updatesPerSecond The maximum number of updates per second produced.
  */
case class EndlessReadService(
    updatesPerSecond: Int,
    name: String,
)(implicit loggingContext: LoggingContext)
    extends ReadService
    with AutoCloseable {
  import EndlessReadService._

  private val logger = ContextualizedLogger.get(this.getClass)

  override def currentHealth(): HealthStatus = synchronized {
    if (aborted) HealthStatus.unhealthy else HealthStatus.healthy
  }

  override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] = synchronized {
    logger.info("EndlessReadService.ledgerInitialConditions() called")
    initialConditionCalls += 1
    Source
      .single(LedgerInitialConditions(ledgerId, configuration, recordTime(0)))
      .via(killSwitch.flow)
  }

  /** Produces the following stream of updates:
    *    1. a config change
    *    1. a party allocation
    *    1. a package upload
    *    1. a transaction that creates a contract
    *    1. a transaction that archives the previous contract
    *
    *    The last two items above repeat indefinitely
    */
  override def stateUpdates(
      beginAfter: Option[Offset]
  )(implicit loggingContext: LoggingContext): Source[(Offset, Update), NotUsed] =
    synchronized {
      logger.info(s"EndlessReadService.stateUpdates($beginAfter) called")
      stateUpdatesCalls += 1
      val startIndex: Int = beginAfter.map(index).getOrElse(0) + 1
      Source
        .fromIterator(() => Iterator.from(startIndex))
        .map {
          case i @ 1 =>
            offset(i) -> Update.ConfigurationChanged(
              recordTime(i),
              submissionId(i),
              participantId,
              configuration,
            )
          case i @ 2 =>
            offset(i) -> Update.PartyAddedToParticipant(
              party,
              "Operator",
              participantId,
              recordTime(i),
              Some(submissionId(i)),
            )
          case i @ 3 =>
            offset(i) -> Update.PublicPackageUpload(
              List(archive),
              Some("Package"),
              recordTime(i),
              Some(submissionId(i)),
            )
          case i if i % 2 == 0 =>
            offset(i) -> Update.TransactionAccepted(
              optCompletionInfo = Some(completionInfo(i)),
              transactionMeta = transactionMeta(i),
              transaction = createTransaction(i),
              transactionId = transactionId(i),
              recordTime = recordTime(i),
              divulgedContracts = List.empty,
              blindingInfo = None,
              contractMetadata = Map.empty,
            )
          case i =>
            offset(i) -> Update.TransactionAccepted(
              optCompletionInfo = Some(completionInfo(i)),
              transactionMeta = transactionMeta(i),
              transaction = exerciseTransaction(i),
              transactionId = transactionId(i),
              recordTime = recordTime(i),
              divulgedContracts = List.empty,
              blindingInfo = None,
              contractMetadata = Map.empty,
            )
        }
        .via(killSwitch.flow)
    }

  def abort(cause: Throwable): Unit = synchronized {
    logger.info(s"EndlessReadService.abort() called")
    aborted = true
    killSwitch.abort(cause)
  }

  def reset(): Unit = synchronized {
    assert(aborted)
    logger.info(s"EndlessReadService.reset() called")
    stateUpdatesCalls = 0
    initialConditionCalls = 0
    aborted = false
    killSwitch = KillSwitches.shared("EndlessReadService")
  }

  override def close(): Unit = synchronized {
    logger.info(s"EndlessReadService.close() called")
    killSwitch.shutdown()
  }

  def isRunning: Boolean = synchronized {
    stateUpdatesCalls > 0 && !aborted
  }

  private var stateUpdatesCalls: Int = 0
  private var initialConditionCalls: Int = 0
  private var aborted: Boolean = false
  private var killSwitch = KillSwitches.shared("EndlessReadService")
}

object EndlessReadService {
  val ledgerId: LedgerId = "EndlessReadService"
  val participantId: Ref.ParticipantId =
    Ref.ParticipantId.assertFromString("EndlessReadServiceParticipant")
  val configuration: Configuration = Configuration.reasonableInitialConfiguration
  val party: Ref.Party = Ref.Party.assertFromString("operator")
  val applicationId: Ref.ApplicationId = Ref.ApplicationId.assertFromString("Application")
  val workflowId: Ref.WorkflowId = Ref.WorkflowId.assertFromString("Workflow")
  val templateId: Ref.Identifier = Ref.Identifier.assertFromString("pkg:Mod:Template")
  val choiceName: Ref.Name = Ref.Name.assertFromString("SomeChoice")
  val statistics: TransactionNodeStatistics = TransactionNodeStatistics.Empty

  private val archive = DamlLf.Archive.newBuilder
    .setHash("00001")
    .setHashFunction(DamlLf.HashFunction.SHA256)
    .setPayload(ByteString.copyFromUtf8("payload 1"))
    .build

  // Note: all methods in this object MUST be fully deterministic
  def index(o: Offset): Int = Integer.parseInt(o.toHexString, 16)
  def offset(i: Int): Offset = Offset.fromHexString(Ref.HexString.assertFromString(f"$i%08x"))
  def submissionId(i: Int): Ref.SubmissionId = Ref.SubmissionId.assertFromString(f"sub$i%08x")
  def transactionId(i: Int): Ref.TransactionId = Ref.TransactionId.assertFromString(f"tx$i%08x")
  def commandId(i: Int): Ref.CommandId = Ref.CommandId.assertFromString(f"cmd$i%08x")
  def cid(i: Int): Value.ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey(i.toString))
  def recordTime(i: Int): Timestamp =
    Timestamp.assertFromInstant(Instant.EPOCH.plusSeconds(i.toLong))
  def completionInfo(i: Int): CompletionInfo = CompletionInfo(
    actAs = List(party),
    applicationId = applicationId,
    commandId = commandId(i),
    optDeduplicationPeriod = None,
    submissionId = None,
    statistics = Some(statistics),
  )
  def transactionMeta(i: Int): TransactionMeta = TransactionMeta(
    ledgerEffectiveTime = recordTime(i),
    workflowId = Some(workflowId),
    submissionTime = recordTime(i),
    submissionSeed = crypto.Hash.hashPrivateKey("EndlessReadService"),
    optUsedPackages = None,
    optNodeSeeds = None,
    optByKeyNodes = None,
  )
  // Creates contract #i
  private def createTransaction(i: Int): CommittedTransaction = {
    val builder = TransactionBuilder()
    val createNode = builder.create(
      id = cid(i),
      templateId = templateId,
      argument = Value.ValueUnit,
      signatories = Set(party),
      observers = Set.empty,
      key = None,
    )
    builder.add(createNode)
    builder.buildCommitted()
  }
  // Archives contract #(i-1)
  private def exerciseTransaction(i: Int): CommittedTransaction = {
    val builder = TransactionBuilder()
    val createNode = builder.create(
      id = cid(i - 1),
      templateId = templateId,
      argument = Value.ValueUnit,
      signatories = Set(party),
      observers = Set.empty,
      key = None,
    )
    val exerciseNode = builder.exercise(
      contract = createNode,
      choice = choiceName,
      consuming = true,
      actingParties = Set(party),
      argument = Value.ValueUnit,
      result = Some(Value.ValueUnit),
      choiceObservers = Set.empty,
      byKey = false,
    )
    builder.add(exerciseNode)
    builder.buildCommitted()
  }
}
