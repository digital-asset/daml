// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.ha

import cats.syntax.bifunctor.toBifunctorOps
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.participant.state.{
  CompletionInfo,
  InternalStateServiceProviderImpl,
  ReadService,
  TransactionMeta,
  Update,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext.wrapWithNewTraceContext
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.CommittedTransaction
import com.digitalasset.daml.lf.transaction.test.{TestNodeBuilder, TreeTransactionBuilder}
import com.digitalasset.daml.lf.value.Value
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.Source

import java.time.Instant
import scala.concurrent.blocking

/** An infinite stream of state updates that fully conforms to the Daml ledger model.
  *
  *  All instances of this class produce the same stream of state updates.
  *
  *  @param updatesPerSecond The maximum number of updates per second produced.
  */
final case class EndlessReadService(
    updatesPerSecond: Int,
    name: String,
    loggerFactory: NamedLoggerFactory,
)(implicit traceContext: TraceContext)
    extends ReadService
    with NamedLogging
    with AutoCloseable
    with InternalStateServiceProviderImpl {
  import EndlessReadService.*

  override def currentHealth(): HealthStatus = blocking(
    synchronized(
      if (aborted) HealthStatus.unhealthy else HealthStatus.healthy
    )
  )

  /** Produces the following stream of updates:
    *    1. an init update
    *    1. a party allocation
    *    1. a package upload
    *    1. a transaction that creates a contract
    *    1. a transaction that archives the previous contract
    *
    *    The last two items above repeat indefinitely
    */
  override def stateUpdates(
      beginAfter: Option[Offset]
  )(implicit traceContext: TraceContext): Source[(Offset, Traced[Update]), NotUsed] =
    blocking(synchronized {
      logger.info(s"EndlessReadService.stateUpdates($beginAfter) called")
      stateUpdatesCalls += 1
      val startIndex: Int = beginAfter.map(index).getOrElse(0) + 1
      Source
        .fromIterator(() => Iterator.from(startIndex))
        .map {
          case i @ 1 =>
            offset(i) -> Update.Init(
              recordTime(i)
            )
          case i @ 2 =>
            offset(i) -> Update.PartyAddedToParticipant(
              party,
              "Operator",
              participantId,
              recordTime(i),
              Some(submissionId(i)),
            )
          // On odd, create a contract
          case i if i % 2 == 1 =>
            offset(i) -> Update.TransactionAccepted(
              completionInfoO = Some(completionInfo(i)),
              transactionMeta = transactionMeta(i),
              transaction = createTransaction(i),
              transactionId = transactionId(i),
              recordTime = recordTime(i),
              blindingInfoO = None,
              hostedWitnesses = Nil,
              contractMetadata = Map.empty,
              domainId = DomainId.tryFromString("da::default"),
              domainIndex = None,
            )
          // On even, exercise a contract
          case i =>
            offset(i) -> Update.TransactionAccepted(
              completionInfoO = Some(completionInfo(i)),
              transactionMeta = transactionMeta(i),
              transaction = exerciseTransaction(i),
              transactionId = transactionId(i),
              recordTime = recordTime(i),
              blindingInfoO = None,
              hostedWitnesses = Nil,
              contractMetadata = Map.empty,
              domainId = DomainId.tryFromString("da::default"),
              domainIndex = None,
            )
        }
        .map(_.bimap(identity, wrapWithNewTraceContext))
        .via(killSwitch.flow)
    })

  def abort(cause: Throwable): Unit = blocking(synchronized {
    logger.info(s"EndlessReadService.abort() called")
    aborted = true
    killSwitch.abort(cause)
  })

  def reset(): Unit = blocking(synchronized {
    assert(aborted)
    logger.info(s"EndlessReadService.reset() called")
    stateUpdatesCalls = 0
    initialConditionCalls = 0
    aborted = false
    killSwitch = KillSwitches.shared("EndlessReadService")
  })

  override def close(): Unit = blocking(synchronized {
    logger.info(s"EndlessReadService.close() called")
    killSwitch.shutdown()
  })

  def isRunning: Boolean = blocking(synchronized {
    stateUpdatesCalls > 0 && !aborted
  })

  private var stateUpdatesCalls: Int = 0
  private var initialConditionCalls: Int = 0
  private var aborted: Boolean = false
  private var killSwitch = KillSwitches.shared("EndlessReadService")
}

object EndlessReadService {
  val participantId: Ref.ParticipantId =
    Ref.ParticipantId.assertFromString("EndlessReadServiceParticipant")
  val party: Ref.Party = Ref.Party.assertFromString("operator")
  val applicationId: Ref.ApplicationId = Ref.ApplicationId.assertFromString("Application")
  val workflowId: Ref.WorkflowId = Ref.WorkflowId.assertFromString("Workflow")
  val templateId: Ref.Identifier = Ref.Identifier.assertFromString("pkg:Mod:Template")
  val choiceName: Ref.Name = Ref.Name.assertFromString("SomeChoice")

  // Note: all methods in this object MUST be fully deterministic
  def index(o: Offset): Int = o.toLong.toInt
  def offset(i: Int): Offset = Offset.fromLong(i.toLong)
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
    messageUuid = None,
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
    val createNode = TestNodeBuilder.create(
      id = cid(i),
      templateId = templateId,
      argument = Value.ValueUnit,
      signatories = Set(party),
      observers = Set.empty,
    )
    TreeTransactionBuilder.toCommittedTransaction(createNode)
  }
  // Archives contract #(i-1)
  private def exerciseTransaction(i: Int): CommittedTransaction = {
    val createNode = TestNodeBuilder.create(
      id = cid(i - 1),
      templateId = templateId,
      argument = Value.ValueUnit,
      signatories = Set(party),
      observers = Set.empty,
    )
    val exerciseNode = TestNodeBuilder.exercise(
      contract = createNode,
      choice = choiceName,
      consuming = true,
      actingParties = Set(party),
      argument = Value.ValueUnit,
      result = Some(Value.ValueUnit),
      choiceObservers = Set.empty,
      byKey = false,
    )
    TreeTransactionBuilder.toCommittedTransaction(exerciseNode)
  }
}
