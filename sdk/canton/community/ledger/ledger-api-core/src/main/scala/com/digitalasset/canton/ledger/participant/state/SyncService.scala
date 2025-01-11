// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.health.ReportsHealth
import com.digitalasset.canton.ledger.participant.state.SyncService.{
  ConnectedSynchronizerRequest,
  ConnectedSynchronizerResponse,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, SynchronizerAlias}

/** An interface to change a ledger via a participant.
  * '''Please note that this interface is unstable and may significantly change.'''
  *
  * The methods in this interface are all methods that are supported
  * *uniformly* across all ledger participant implementations. Methods for
  * uploading packages, on-boarding parties, and changing ledger-wide
  * configuration are specific to a ledger and therefore to a participant
  * implementation. Moreover, these methods usually require admin-level
  * privileges, whose granting is also specific to a ledger.
  *
  * If a ledger is run for testing only, there is the option for quite freely
  * allowing the on-boarding of parties and uploading of packages. There are
  * plans to make this functionality uniformly available: see the roadmap for
  * progress information https://github.com/digital-asset/daml/issues/121.
  *
  * The following methods are currently available for changing the state of a Daml ledger:
  * - submitting a transaction using [[SyncService!.submitTransaction]]
  * - allocating a new party using [[PartySyncService!.allocateParty]]
  * - pruning a participant ledger using [[ParticipantPruningSyncService!.prune]]
  */
trait SyncService
    extends SubmissionSyncService
    with PackageSyncService
    with PartySyncService
    with ParticipantPruningSyncService
    with ReportsHealth
    with InternalStateServiceProvider {

  // temporary implementation, will be removed as topology events on Ledger API proceed
  def getConnectedSynchronizers(request: ConnectedSynchronizerRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ConnectedSynchronizerResponse] =
    throw new UnsupportedOperationException()

  // TODO(i20688): Temporary until prepared transactions run through the domain router
  def getProtocolVersionForSynchronizer(
      synchronizerId: Traced[SynchronizerId]
  ): Option[ProtocolVersion] =
    None

  // temporary implementation, will be removed as topology events on Ledger API proceed
  /** Get the offsets of the incomplete assigned/unassigned events for a set of stakeholders.
    *
    * @param validAt      The offset of validity in participant offset terms.
    * @param stakeholders Only offsets are returned which have at least one stakeholder from this set.
    * @return All the offset of assigned/unassigned events which do not have their counterparts visible at
    *         the validAt offset, and only for the reassignments for which this participant is reassigning.
    */
  def incompleteReassignmentOffsets(
      validAt: Offset,
      stakeholders: Set[LfPartyId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Vector[Offset]] = {
    val _ = validAt
    val _ = stakeholders
    val _ = traceContext
    FutureUnlessShutdown.pure(Vector.empty)
  }
}

object SyncService {
  final case class ConnectedSynchronizerRequest(
      party: LfPartyId,
      participantId: Option[ParticipantId],
  )

  final case class ConnectedSynchronizerResponse(
      connectedSynchronizers: Seq[ConnectedSynchronizerResponse.ConnectedSynchronizer]
  )

  object ConnectedSynchronizerResponse {
    final case class ConnectedSynchronizer(
        synchronizerAlias: SynchronizerAlias,
        synchronizerId: SynchronizerId,
        permission: ParticipantPermission,
    )
  }
}
