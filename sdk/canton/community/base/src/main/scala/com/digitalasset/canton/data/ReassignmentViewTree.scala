// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{HashOps, Salt}
import com.digitalasset.canton.protocol.{LfTemplateId, SerializableContract, Stakeholders}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}

import java.util.UUID

/** A reassignment (unassignment or assignment) request embedded in a Merkle tree.
  * The view may or may not be blinded.
  */
trait ReassignmentViewTree {
  def commonData: MerkleTreeLeaf[ReassignmentCommonData]

  def view: MerkleTree[ReassignmentView]
  // Use a dummy value, as there is only one view.
  def viewPosition: ViewPosition = ViewPosition.root

  def submittingParticipant: ParticipantId =
    commonData.tryUnwrap.submitterMetadata.submittingParticipant
}

/** Aggregates the data of an assignment request that is sent to the mediator and the involved participants.
  */
trait ReassignmentCommonData extends ProtocolVersionedMemoizedEvidence {
  def salt: Salt
  def stakeholders: Stakeholders
  def uuid: UUID
  def submitterMetadata: ReassignmentSubmitterMetadata
  def reassigningParticipants: Set[ParticipantId]

  def hashOps: HashOps

  def confirmingParties: Map[LfPartyId, PositiveInt] =
    stakeholders.signatories.map(_ -> PositiveInt.one).toMap
}

trait ReassignmentView extends ProtocolVersionedMemoizedEvidence {
  def salt: Salt
  def contract: SerializableContract
  def reassignmentCounter: ReassignmentCounter

  def templateId: LfTemplateId =
    contract.rawContractInstance.contractInstance.unversioned.template
}
