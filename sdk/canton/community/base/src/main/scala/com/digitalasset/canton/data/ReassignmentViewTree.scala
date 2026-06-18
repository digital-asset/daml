// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{HashOps, Salt}
import com.digitalasset.canton.protocol.Stakeholders
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.ParticipantId

import java.util.UUID

/** A reassignment (unassignment or assignment) request embedded in a Merkle tree. The view may or
  * may not be blinded.
  */
trait ReassignmentViewTree {
  def commonData: MerkleTreeLeaf[ReassignmentCommonData]

  def view: MerkleTree[ReassignmentView]
  // Use a dummy value, as there is only one view.
  def viewPosition: ViewPosition = ViewPosition.root

  def submittingParticipant: ParticipantId =
    commonData.tryUnwrap.submitterMetadata.submittingParticipant
}

/** Aggregates the data of an assignment request that is sent to the mediator and the involved
  * participants.
  */
trait ReassignmentCommonData extends ProtocolVersionedMemoizedEvidence {
  def salt: Salt
  def stakeholders: Stakeholders
  def uuid: UUID
  def submitterMetadata: ReassignmentSubmitterMetadata
  def reassigningParticipants: Set[ParticipantId]

  def hashOps: HashOps

  // The admin party of the submitting participant is passed as an extra confirming party to guarantee proper authorization.
  def confirmingParties: Set[LfPartyId] =
    stakeholders.signatories + submitterMetadata.submittingAdminParty
}

trait ReassignmentView extends ProtocolVersionedMemoizedEvidence {
  def salt: Salt
  def contracts: ContractsReassignmentBatch
}
