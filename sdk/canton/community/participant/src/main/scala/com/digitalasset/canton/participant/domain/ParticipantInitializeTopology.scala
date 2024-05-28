// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.domain.{
  RegisterTopologyTransactionHandle,
  SequencerBasedRegisterTopologyTransactionHandle,
}
import com.digitalasset.canton.config.{DomainTimeTrackerConfig, ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.participant.topology.DomainOnboardingOutbox
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.RequestSigner.UnauthenticatedRequestSigner
import com.digitalasset.canton.sequencing.client.{SequencerClient, SequencerClientFactory}
import com.digitalasset.canton.sequencing.handlers.DiscardIgnoredEvents
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.{AuthorizedStore, DomainStore}
import com.digitalasset.canton.topology.{
  DomainId,
  ParticipantId,
  SequencerId,
  UnauthenticatedMemberId,
}
import com.digitalasset.canton.tracing.TraceContext
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
final class ParticipantInitializeTopology(
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
    config: TopologyConfig,
    protocolVersion: ProtocolVersion,
    expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
) {
  def run()(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
      loggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
    val unauthenticatedMember =
      UnauthenticatedMemberId.tryCreate(participantId.namespace)(crypto.pureCrypto)

    loggingContext.logger.debug(
      s"Unauthenticated member $unauthenticatedMember will register initial topology transactions on behalf of participant $participantId"
    )

    def pushTopologyAndVerify(
        client: SequencerClient,
        domainTimeTracker: DomainTimeTracker,
    ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
      val handle = createHandler(client)

      for {
        _ <- EitherT.right[DomainRegistryError](
          FutureUnlessShutdown.outcomeF(
            // using send tracking requires a subscription, otherwise the send tracker
            // doesn't get updated
            client.subscribeAfterUnauthenticated(
              CantonTimestamp.MinValue,
              // There is no point in ignoring events in an unauthenticated subscription
              DiscardIgnoredEvents(loggerFactory)(
                ApplicationHandler.success(s"participant-initialize-topology-$alias")
              ),
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
        processingTimeout,
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

  private def createHandler(
      client: SequencerClient
  )(implicit ec: ExecutionContext): RegisterTopologyTransactionHandle =
    new SequencerBasedRegisterTopologyTransactionHandle(
      client,
      domainId,
      participantId,
      clock,
      config,
      protocolVersion,
      processingTimeout,
      loggerFactory,
    )

  private def initiateOnboarding(
      handle: RegisterTopologyTransactionHandle
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
