// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.{
  ExternalPartyOnboardingDetails,
  ParticipantId,
  PartyId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Ref

/** An interface for on-boarding parties via a participant. */
trait PartySyncService {

  /** Adds a new party to the set managed by the ledger.
    *
    * Caller specifies a party identifier suggestion, the actual identifier allocated might be
    * different and is implementation specific.
    *
    * In particular, a ledger may:
    *   - Disregard the given hint and choose a completely new party identifier
    *   - Construct a new unique identifier from the given hint, e.g., by appending a UUID
    *   - Use the given hint as is, and reject the call if such a party already exists
    *
    * Successful party allocations will result in a
    * [[com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective]]
    * message. See the comments on [[com.digitalasset.canton.ledger.participant.state.Update]] for
    * further details.
    *
    * @param hint
    *   A party identifier suggestion
    * @param submissionId
    *   Client picked submission identifier for matching the responses with the request.
    * @param synchronizerIdO
    *   The synchronizer on which the party should be allocated. Can be omitted if the participant
    *   is connected to only one synchronizer.
    * @param externalPartyOnboardingDetails
    *   Onboarding information when allocating an external party
    * @return
    *   an async result of a SubmissionResult
    */
  def allocateParty(
      partyId: PartyId,
      submissionId: Ref.SubmissionId,
      synchronizerIdO: Option[SynchronizerId],
      externalPartyOnboardingDetails: Option[ExternalPartyOnboardingDetails],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SubmissionResult]

  /** Return the protocol version for a synchronizer ID if the node is connected to it.
    */
  def protocolVersionForSynchronizerId(synchronizerId: SynchronizerId): Option[ProtocolVersion]

  /** The participant id */
  def participantId: ParticipantId

  /** Hash ops of the participant */
  def hashOps: HashOps

}
