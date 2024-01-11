// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.initialization

import cats.data.EitherT
import cats.instances.future.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{
  Crypto,
  DomainSnapshotSyncCryptoApi,
  DomainSyncCryptoClient,
  PublicKey,
}
import com.digitalasset.canton.domain.config.{DomainBaseConfig, DomainConfig}
import com.digitalasset.canton.domain.topology.client.DomainInitializationObserver
import com.digitalasset.canton.domain.topology.store.RegisterTopologyTransactionResponseStore
import com.digitalasset.canton.domain.topology.{DomainTopologyManagerEventHandler, *}
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.DomainTopologyTransactionMessage
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.{
  RequestSigner,
  SendAsyncClientError,
  SendType,
  SequencerClient,
  SequencerClientFactory,
}
import com.digitalasset.canton.sequencing.handlers.{
  DiscardIgnoredEvents,
  EnvelopeOpener,
  StripSignature,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope, Recipients}
import com.digitalasset.canton.sequencing.{SequencerConnections, UnsignedEnvelopeBox}
import com.digitalasset.canton.store.db.SequencerClientDiscriminator
import com.digitalasset.canton.store.{
  IndexedStringStore,
  SendTrackerStore,
  SequencedEventStore,
  SequencerCounterTrackerStore,
}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

final case class TopologyManagementComponents(
    domainTopologyServiceHandler: DomainTopologyManagerEventHandler,
    client: DomainTopologyClientWithInit,
    sequencerClient: SequencerClient,
    processor: TopologyTransactionProcessor,
    dispatcher: DomainTopologyDispatcher,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
) extends FlagCloseable
    with NamedLogging {

  override def onClosed(): Unit =
    Lifecycle.close(domainTopologyServiceHandler, dispatcher, client, processor)(logger)

}

object TopologyManagementInitialization {
  val topologySenderHealthName: String = "domain-topology-sender"

  def sequenceInitialTopology(
      id: DomainId,
      protocolVersion: ProtocolVersion,
      client: SequencerClient,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      domainMembers: Set[DomainMember],
      recentSnapshot: DomainSnapshotSyncCryptoApi,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): Future[Unit] = {
    implicit val traceContext = loggingContext.traceContext
    val logger = loggerFactory.getLogger(getClass)

    for {
      _ <- SequencerClient
        .sendWithRetries(
          callback => {
            val maxSequencingTime = client.generateMaxSequencingTime
            logger.info(
              s"Sending initial topology transactions to domain members $domainMembers as ${recentSnapshot.owner}"
            )
            for {
              content <-
                DomainTopologyTransactionMessage
                  .create(
                    transactions.toList,
                    recentSnapshot,
                    id,
                    Some(maxSequencingTime),
                    protocolVersion,
                  )
                  .leftMap(SendAsyncClientError.RequestInvalid)
              batch = domainMembers.map(member =>
                OpenEnvelope(content, Recipients.cc(member))(protocolVersion)
              )
              res <- client
                .sendAsync(
                  Batch(batch.toList, protocolVersion),
                  SendType.Other,
                  callback = callback,
                  maxSequencingTime = maxSequencingTime,
                )
            } yield res
          },
          maxRetries = 600,
          delay = 1.second,
          sendDescription = "Send initial topology transaction to domain members",
          errMsg = "Failed to send initial topology transactions to domain members",
          performUnlessClosing = client,
        )
        .onShutdown(
          logger.debug("sequenceInitialTopology aborted due to shutdown")
        )
    } yield ()
  }

  def apply(
      config: DomainBaseConfig,
      id: DomainId,
      storage: Storage,
      clock: Clock,
      crypto: Crypto,
      syncCrypto: DomainSyncCryptoClient,
      sequencedTopologyStore: TopologyStore[TopologyStoreId.DomainStore],
      sequencerConnections: SequencerConnections,
      domainTopologyManager: DomainTopologyManager,
      domainTopologyService: DomainTopologyManagerRequestService,
      topologyManagerSequencerCounterTrackerStore: SequencerCounterTrackerStore,
      topologyProcessor: TopologyTransactionProcessor,
      topologyClient: DomainTopologyClientWithInit,
      initialKeys: Map[KeyOwner, Seq[PublicKey]],
      sequencerClientFactory: SequencerClientFactory,
      parameters: CantonNodeParameters,
      futureSupervisor: FutureSupervisor,
      indexedStringStore: IndexedStringStore,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      tracer: Tracer,
      loggingContext: ErrorLoggingContext,
  ): EitherT[Future, String, TopologyManagementComponents] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext

    val managerId: DomainTopologyManagerId = domainTopologyManager.id
    val timeouts = parameters.processingTimeouts
    val protocolVersion = domainTopologyManager.protocolVersion
    val dispatcherLoggerFactory = loggerFactory.appendUnnamedKey("node", "identity")
    // if we're only running a embedded sequencer then there's no need to sequence topology transactions
    val isEmbedded = config match {
      case _: DomainConfig => true
      case _ => false
    }
    val addressSequencerAsDomainMember = !isEmbedded
    for {
      managerDiscriminator <- EitherT.right(
        SequencerClientDiscriminator.fromDomainMember(managerId, indexedStringStore)
      )
      newClient <- {
        val sequencedEventStore =
          SequencedEventStore(
            storage,
            managerDiscriminator,
            protocolVersion,
            timeouts,
            dispatcherLoggerFactory,
          )
        val sendTrackerStore = SendTrackerStore(storage)
        sequencerClientFactory.create(
          managerId,
          sequencedEventStore,
          sendTrackerStore,
          RequestSigner(syncCrypto, protocolVersion),
          sequencerConnections,
          NonEmpty.mk(Set, SequencerAlias.Default -> SequencerId(id)).toMap,
        )
      }
      timeTracker = DomainTimeTracker(
        config.timeTracker,
        clock,
        newClient,
        protocolVersion,
        timeouts,
        loggerFactory,
      )
      domainTopologyServiceHandler =
        new DomainTopologyManagerEventHandler(
          RegisterTopologyTransactionResponseStore(
            storage,
            crypto.pureCrypto,
            protocolVersion,
            timeouts,
            loggerFactory,
          ),
          domainTopologyService,
          newClient,
          protocolVersion,
          timeouts,
          loggerFactory,
        )
      eventHandler = {
        val topologyProcessorHandler = topologyProcessor.createHandler(id)
        DiscardIgnoredEvents(loggerFactory) {
          StripSignature {
            EnvelopeOpener[UnsignedEnvelopeBox](
              protocolVersion,
              crypto.pureCrypto,
            ) {
              domainTopologyServiceHandler.combineWith(topologyProcessorHandler)
            }
          }
        }
      }

      _ <- EitherT.right[String](
        newClient.subscribeTracking(
          topologyManagerSequencerCounterTrackerStore,
          eventHandler,
          timeTracker,
        )
      )

      initializationObserver <- EitherT.right(
        DomainInitializationObserver(
          id,
          topologyClient,
          sequencedTopologyStore,
          // by our definition / convention, an embedded domain runs all domain nodes at once
          // hence, we require an active mediator before it is properly initialised.
          // in contrast, a distributed domain can start without a domain manager (but
          // then participants won't be able to submit transactions until we have a mediator)
          mustHaveActiveMediator = isEmbedded,
          timeouts,
          loggerFactory,
        )
      )
      // before starting the domain identity dispatcher, we need to make sure the initial topology transactions
      // have been sequenced. in the case of external sequencers this is done with admin commands and we just need to wait,
      // but for embedded sequencers we need to explicitly sequence these transactions here if that's not already been done.
      hasInitData <- EitherT.right(initializationObserver.initialisedAtHead)
      _ <-
        if (isEmbedded && !hasInitData) {
          EitherT.right(for {
            authorizedTopologySnapshot <- domainTopologyManager.store.headTransactions(traceContext)
            _ <- sequenceInitialTopology(
              id,
              protocolVersion,
              newClient,
              authorizedTopologySnapshot.result.map(_.transaction),
              DomainMember.list(id, addressSequencerAsDomainMember),
              syncCrypto.currentSnapshotApproximation,
              loggerFactory,
            )
          } yield ())
        } else EitherT.rightT(())
      _ <- EitherT.right(initializationObserver.waitUntilInitialisedAndEffective.unwrap)
      dispatcher <- EitherT(
        DomainTopologyDispatcher
          .create(
            id,
            domainTopologyManager,
            topologyClient,
            topologyProcessor,
            initialKeys,
            sequencedTopologyStore,
            newClient,
            timeTracker,
            crypto,
            clock,
            addressSequencerAsDomainMember,
            parameters,
            futureSupervisor,
            dispatcherLoggerFactory,
            topologyManagerSequencerCounterTrackerStore,
          )
          .map(Right(_))
          .onShutdown(Left("Initialization aborted due to shutdown"))
      )
    } yield TopologyManagementComponents(
      domainTopologyServiceHandler,
      topologyClient,
      newClient,
      topologyProcessor,
      dispatcher,
      parameters.processingTimeouts,
      loggerFactory,
    )
  }
}
