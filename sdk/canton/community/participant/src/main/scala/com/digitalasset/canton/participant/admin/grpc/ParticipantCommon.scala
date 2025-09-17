// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.admin.data.ActiveContract as ActiveContractValueClass
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ResourceUtil
import org.apache.pekko.actor.ActorSystem

import java.io.OutputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[admin] object ParticipantCommon {

  private[grpc] def findLedgerEnd(sync: CantonSyncService): Either[String, Offset] =
    sync.participantNodePersistentState.value.ledgerApiStore.ledgerEndCache
      .apply()
      .map(_.lastOffset)
      .toRight("No ledger end found")

  /** Writes a snapshot of the Active Contract Set (ACS) for the given parties at a specific offset
    * to a destination stream. .
    *
    * @param indexService
    *   The service for querying active contracts.
    * @param parties
    *   The set of parties used to query the initial ACS.
    * @param atOffset
    *   The ledger offset for the snapshot.
    * @param destination
    *   The output stream for the serialized contracts.
    * @param excludedStakeholders
    *   Excludes any contract where a stakeholder is in this set.
    * @param synchronizerId
    *   Optionally filters contracts by this ID.
    * @param contractSynchronizerRenames
    *   A map to rename synchronizer IDs before writing.
    * @return
    *   A future that completes with `Right(())` on success, or a `Left` with an error message on
    *   failure.
    */
  private[grpc] def writeAcsSnapshot(
      indexService: InternalIndexService,
      parties: Set[PartyId],
      atOffset: Offset,
      destination: OutputStream,
      excludedStakeholders: Set[PartyId] = Set.empty,
      synchronizerId: Option[SynchronizerId] = None,
      contractSynchronizerRenames: Map[String, String] = Map.empty,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      actorSystem: ActorSystem,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _ <- EitherT
        .apply[Future, String, Unit](
          ResourceUtil.withResourceM(destination)(out =>
            indexService
              .activeContracts(parties.map(_.toLf), Some(atOffset))
              .map(response => response.getActiveContract)
              .filter(contract =>
                synchronizerId
                  .forall(filterId => contract.synchronizerId == filterId.toProtoPrimitive)
              )
              .filter { contract =>
                val event = contract.getCreatedEvent
                val stakeholders = (event.signatories ++ event.observers).toSet
                val excludeStakeholdersS = excludedStakeholders.map(_.toProtoPrimitive)
                excludeStakeholdersS.intersect(stakeholders).isEmpty
              }
              .map { contract =>
                if (contractSynchronizerRenames.contains(contract.synchronizerId)) {
                  val synchronizerId = contractSynchronizerRenames
                    .getOrElse(contract.synchronizerId, contract.synchronizerId)
                  contract.copy(synchronizerId = synchronizerId)
                } else {
                  contract
                }
              }
              .map(ActiveContractValueClass.tryCreate)
              .map {
                _.writeDelimitedTo(out) match {
                  // throwing intentionally to immediately interrupt any further Pekko source stream processing
                  case Left(errorMessage) => throw new RuntimeException(errorMessage)
                  case Right(_) => out.flush()
                }
              }
              .run()
              .transform {
                case Failure(e) => Success(Left(e.getMessage)) // a Pekko stream error
                case Success(_) => Success(Right(()))
              }
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield ()

}
