// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.domain.{
  RegisterTopologyTransactionHandleWithProcessor,
  SequencerBasedRegisterTopologyTransactionHandle,
  SequencerBasedRegisterTopologyTransactionHandleX,
}
import com.digitalasset.canton.config.{DomainTimeTrackerConfig, ProcessingTimeout, TopologyXConfig}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.participant.topology.{
  DomainOnboardingOutbox,
  DomainOnboardingOutboxX,
}
import com.digitalasset.canton.protocol.messages.{
  RegisterTopologyTransactionResponseResult,
  TopologyTransactionsBroadcastX,
}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.RequestSigner.UnauthenticatedRequestSigner
import com.digitalasset.canton.sequencing.client.{SequencerClient, SequencerClientFactory}
import com.digitalasset.canton.sequencing.handlers.DiscardIgnoredEvents
import com.digitalasset.canton.sequencing.protocol.{Batch, ClosedEnvelope, Deliver, SequencedEvent}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.store.TopologyStoreId.{AuthorizedStore, DomainStore}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.topology.{
  DomainId,
  ParticipantId,
  SequencerId,
  UnauthenticatedMemberId,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, SequencerAlias}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/** Takes care of requesting approval of the participant's initial topology transactions to the IDM via the sequencer.
  * Before these transactions have been approved, the participant cannot connect to the sequencer because it can't
  * authenticate without the IDM having approved the transactions. Because of that, this initial request is sent by
  * a dynamically created unauthenticated member whose sole purpose is to send this request and wait for the response.
  */
abstract class ParticipantInitializeTopologyCommon[TX, State](
    alias: DomainAlias,
    participantId: ParticipantId,
    clock: Clock,
    timeouts: ProcessingTimeout,
    timeTracker: DomainTimeTrackerConfig,
    loggerFactory: NamedLoggerFactory,
    sequencerClientFactory: SequencerClientFactory,
    connections: SequencerConnections,
    crypto: Crypto,
    protocolVersion: ProtocolVersion,
    expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
) {

  protected def createHandler(
      client: SequencerClient,
      member: UnauthenticatedMemberId,
  )(implicit ec: ExecutionContext): RegisterTopologyTransactionHandleWithProcessor[TX, State]

  protected def initiateOnboarding(
      handle: RegisterTopologyTransactionHandleWithProcessor[TX, State]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean]

  def run()(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
      loggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
    val unauthenticatedMember =
      UnauthenticatedMemberId.tryCreate(participantId.uid.namespace)(crypto.pureCrypto)

    loggingContext.logger.debug(
      s"Unauthenticated member $unauthenticatedMember will register initial topology transactions on behalf of participant $participantId"
    )

    def pushTopologyAndVerify(client: SequencerClient, domainTimeTracker: DomainTimeTracker) = {
      val handle = createHandler(client, unauthenticatedMember)

      val eventHandler = new OrdinaryApplicationHandler[ClosedEnvelope] {
        override def name: String = s"participant-initialize-topology-$alias"

        override def subscriptionStartsAt(
            start: SubscriptionStart,
            domainTimeTracker: DomainTimeTracker,
        )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
          FutureUnlessShutdown.unit

        override def apply(
            tracedEvents: Traced[Seq[BoxedEnvelope[OrdinarySequencedEvent, ClosedEnvelope]]]
        ): HandlerResult = {
          val openEvents = tracedEvents.value.map { closedSignedEvent =>
            val closedEvent = closedSignedEvent.signedEvent.content
            val (openEvent, openingErrors) = {
              SequencedEvent.openEnvelopes(closedEvent)(protocolVersion, crypto.pureCrypto)
            }

            openingErrors.foreach { error =>
              val cause =
                s"Received an envelope at ${closedEvent.timestamp} that cannot be opened. " +
                  s"Discarding envelope... Reason: $error"
              SyncServiceAlarm.Warn(cause).report()
            }

            Traced(openEvent)(closedSignedEvent.traceContext)
          }

          MonadUtil.sequentialTraverseMonoid(openEvents) {
            _.withTraceContext { implicit traceContext =>
              {
                case Deliver(_, _, _, _, batch) => handle.processor(Traced(batch.envelopes))
                case _ => HandlerResult.done
              }
            }
          }
        }
      }

      for {
        _ <- EitherT.right[DomainRegistryError](
          FutureUnlessShutdown.outcomeF(
            client.subscribeAfterUnauthenticated(
              CantonTimestamp.MinValue,
              // There is no point in ignoring events in an unauthenticated subscription
              DiscardIgnoredEvents(loggerFactory)(eventHandler),
              domainTimeTracker,
            )
          )
        )
        // push the initial set of topology transactions to the domain and stop using the unauthenticated member
        success <- initiateOnboarding(handle)
      } yield success
    }

    for {
      unauthenticatedSequencerClient <- sequencerClientFactory
        .create(
          unauthenticatedMember,
          new InMemorySequencedEventStore(loggerFactory),
          new InMemorySendTrackerStore(),
          UnauthenticatedRequestSigner,
          connections,
          expectedSequencers, // TODO(i12906): Iterate over sequencers until the honest answer is received
        )
        .leftMap[DomainRegistryError](
          DomainRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(_)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      domainTimeTracker = DomainTimeTracker(
        timeTracker,
        clock,
        unauthenticatedSequencerClient,
        protocolVersion,
        timeouts,
        loggerFactory,
      )

      success <- {
        def closeEverything(): Future[Unit] = {
          unauthenticatedSequencerClient.close()
          domainTimeTracker.close()
          Future.unit
        }

        EitherT {
          FutureUnlessShutdown {
            pushTopologyAndVerify(unauthenticatedSequencerClient, domainTimeTracker).value.unwrap
              .transformWith {
                case Failure(exception) =>
                  // Close everything and then return the original failure
                  closeEverything().flatMap(_ => Future.failed(exception))
                case Success(value) =>
                  // Close everything and then return the result
                  closeEverything().map(_ => value)
              }
          }
        }
      }
    } yield {
      success
    }
  }
}

class ParticipantInitializeTopology(
    domainId: DomainId,
    alias: DomainAlias,
    participantId: ParticipantId,
    authorizedStore: TopologyStore[AuthorizedStore],
    targetStore: TopologyStore[DomainStore],
    clock: Clock,
    timeTracker: DomainTimeTrackerConfig,
    processingTimeout: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    sequencerClientFactory: SequencerClientFactory,
    connections: SequencerConnections,
    crypto: Crypto,
    protocolVersion: ProtocolVersion,
    expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
) extends ParticipantInitializeTopologyCommon[SignedTopologyTransaction[
      TopologyChangeOp
    ], RegisterTopologyTransactionResponseResult.State](
      alias,
      participantId,
      clock,
      processingTimeout,
      timeTracker,
      loggerFactory,
      sequencerClientFactory,
      connections,
      crypto,
      protocolVersion,
      expectedSequencers,
    ) {

  override protected def createHandler(
      client: SequencerClient,
      member: UnauthenticatedMemberId,
  )(implicit
      ec: ExecutionContext
  ): RegisterTopologyTransactionHandleWithProcessor[SignedTopologyTransaction[
    TopologyChangeOp
  ], RegisterTopologyTransactionResponseResult.State] =
    new SequencerBasedRegisterTopologyTransactionHandle(
      (traceContext, env) =>
        client.sendAsyncUnauthenticated(
          Batch(List(env), protocolVersion)
        )(traceContext),
      domainId,
      participantId,
      member,
      protocolVersion,
      processingTimeout,
      loggerFactory,
    )

  protected def initiateOnboarding(
      handle: RegisterTopologyTransactionHandleWithProcessor[
        SignedTopologyTransaction[TopologyChangeOp],
        RegisterTopologyTransactionResponseResult.State,
      ]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] =
    DomainOnboardingOutbox
      .initiateOnboarding(
        alias,
        domainId,
        protocolVersion,
        participantId,
        handle,
        authorizedStore,
        targetStore,
        processingTimeout,
        loggerFactory,
        crypto,
      )

}

class ParticipantInitializeTopologyX(
    domainId: DomainId,
    alias: DomainAlias,
    participantId: ParticipantId,
    authorizedStore: TopologyStoreX[AuthorizedStore],
    targetStore: TopologyStoreX[DomainStore],
    clock: Clock,
    timeTracker: DomainTimeTrackerConfig,
    processingTimeout: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    sequencerClientFactory: SequencerClientFactory,
    connections: SequencerConnections,
    crypto: Crypto,
    config: TopologyXConfig,
    protocolVersion: ProtocolVersion,
    expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
) extends ParticipantInitializeTopologyCommon[
      GenericSignedTopologyTransactionX,
      TopologyTransactionsBroadcastX.State,
    ](
      alias,
      participantId,
      clock,
      processingTimeout,
      timeTracker,
      loggerFactory,
      sequencerClientFactory,
      connections,
      crypto,
      protocolVersion,
      expectedSequencers,
    ) {

  override protected def createHandler(
      client: SequencerClient,
      member: UnauthenticatedMemberId,
  )(implicit
      ec: ExecutionContext
  ): RegisterTopologyTransactionHandleWithProcessor[
    GenericSignedTopologyTransactionX,
    TopologyTransactionsBroadcastX.State,
  ] =
    new SequencerBasedRegisterTopologyTransactionHandleX(
      client,
      domainId,
      participantId,
      clock,
      config,
      protocolVersion,
      processingTimeout,
      loggerFactory,
    )

  protected def initiateOnboarding(
      handle: RegisterTopologyTransactionHandleWithProcessor[
        GenericSignedTopologyTransactionX,
        TopologyTransactionsBroadcastX.State,
      ]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] =
    DomainOnboardingOutboxX
      .initiateOnboarding(
        alias,
        domainId,
        protocolVersion,
        participantId,
        handle,
        authorizedStore,
        targetStore,
        processingTimeout,
        loggerFactory,
        crypto,
      )

}
