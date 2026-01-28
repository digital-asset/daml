// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.Eval
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.admin.party.{
  PartyReplicationStatus,
  PartyReplicationTestInterceptor,
}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.store.{
  AcsReplicationProgress,
  ParticipantNodePersistentState,
}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, blocking}

class PartyReplicationFileImporter(
    partyId: PartyId,
    requestId: AddPartyRequestId,
    protected val psid: PhysicalSynchronizerId,
    partyOnboardingAt: EffectiveTime,
    protected val replicationProgressState: AcsReplicationProgress,
    persistsContracts: TargetParticipantAcsPersistence.PersistsContracts,
    recordOrderPublisher: RecordOrderPublisher,
    requestTracker: RequestTracker,
    pureCrypto: CryptoPureApi,
    acsReader: Iterator[ActiveContract],
    testOnlyInterceptorO: Option[PartyReplicationTestInterceptor],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TargetParticipantAcsPersistence(
      partyId,
      requestId,
      psid,
      partyOnboardingAt,
      replicationProgressState,
      persistsContracts,
      recordOrderPublisher,
      requestTracker,
      pureCrypto,
    ) {

  def importEntireAcsSnapshotInOneGo()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = for {
    _ <- MonadUtil.sequentialTraverse_(
      acsReader.grouped(TargetParticipantAcsPersistence.contractsToRequestEachTime.unwrap)
    ) { contracts =>
      awaitTestInterceptor()
      val contractsNE =
        NonEmpty
          .from(contracts)
          .getOrElse(throw new IllegalStateException("Grouped ACS must be nonempty"))
      importContracts(contractsNE)
    }
    progress <- EitherT.fromEither[FutureUnlessShutdown](
      replicationProgressState
        .getAcsReplicationProgress(requestId)
        .toRight(s"Party replication $requestId is unexpectedly unknown")
    )
    _ <- replicationProgressState.updateAcsReplicationProgress(
      requestId,
      PartyReplicationStatus.EphemeralFileImporterProgress(
        progress.processedContractCount,
        progress.nextPersistenceCounter,
        fullyProcessedAcs = true,
        this,
      ),
    )
    // Unpause the indexer
    _ <- EitherT.right[String](
      FutureUnlessShutdown.lift(recordOrderPublisher.publishBufferedEvents())
    )
  } yield ()

  private def awaitTestInterceptor()(implicit
      traceContext: TraceContext
  ): Unit = testOnlyInterceptorO.foreach { interceptor =>
    @tailrec
    def go(): Unit =
      replicationProgressState
        .getAcsReplicationProgress(requestId)
        .map(interceptor.onTargetParticipantProgress) match {
        case None => ()
        case Some(PartyReplicationTestInterceptor.Proceed) => ()
        case Some(PartyReplicationTestInterceptor.Wait) =>
          blocking(Threading.sleep(1000))
          go()
      }

    go()
  }

  override protected def newProgress(
      updatedProcessedContractsCount: NonNegativeInt,
      usedRepairCounter: RepairCounter,
  ): PartyReplicationStatus.AcsReplicationProgress =
    PartyReplicationStatus.EphemeralFileImporterProgress(
      updatedProcessedContractsCount,
      usedRepairCounter + 1,
      fullyProcessedAcs = false,
      this,
    )
}

object PartyReplicationFileImporter {
  def apply(
      partyId: PartyId,
      requestId: AddPartyRequestId,
      partyOnboardingAt: EffectiveTime,
      replicationProgressState: AcsReplicationProgress,
      participantNodePersistentState: Eval[ParticipantNodePersistentState],
      connectedSynchronizer: ConnectedSynchronizer,
      acsReader: Iterator[ActiveContract],
      testOnlyInterceptorO: Option[PartyReplicationTestInterceptor],
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext) = new PartyReplicationFileImporter(
    partyId,
    requestId,
    connectedSynchronizer.psid,
    partyOnboardingAt,
    replicationProgressState,
    new TargetParticipantAcsPersistence.PersistsContractsImpl(participantNodePersistentState),
    connectedSynchronizer.ephemeral.recordOrderPublisher,
    connectedSynchronizer.ephemeral.requestTracker,
    connectedSynchronizer.synchronizerHandle.syncPersistentState.pureCryptoApi,
    acsReader,
    testOnlyInterceptorO,
    loggerFactory,
  )
}
