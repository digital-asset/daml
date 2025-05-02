// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{RootHash, Stakeholders, ViewHash}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{LfPartyId, LfWorkflowId}

/** Common supertype of all view trees that are sent as
  * [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]]s
  */
trait ViewTree extends PrettyPrinting {

  /** The informees of the view in the tree */
  def informees: Set[LfPartyId]

  /** Return the hash whose signature is to be included in the
    * [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]]
    */
  def toBeSigned: Option[RootHash]

  /** The hash of the view */
  def viewHash: ViewHash

  def viewPosition: ViewPosition

  /** The root hash of the view tree.
    *
    * Two view trees with the same [[rootHash]] must also have the same [[synchronizerId]] and
    * [[mediator]] (except for hash collisions).
    */
  def rootHash: RootHash

  /** The synchronizer to which the
    * [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]] should be sent to
    */
  def synchronizerId: SynchronizerId

  /** The mediator group that is responsible for coordinating this request */
  def mediator: MediatorGroupRecipient

  override protected def pretty: Pretty[this.type]
}

/** Supertype of [[FullUnassignmentTree]] and [[FullAssignmentTree]]
  */
trait FullReassignmentViewTree extends ViewTree {
  def tree: ReassignmentViewTree
  protected[this] def commonData: ReassignmentCommonData
  protected[this] def view: ReassignmentView

  def reassignmentRef: ReassignmentRef

  val viewPosition: ViewPosition =
    ViewPosition.root // Use a dummy value, as there is only one view.

  def sourceSynchronizer: Source[SynchronizerId]
  def targetSynchronizer: Target[SynchronizerId]

  // Submissions
  def submitterMetadata: ReassignmentSubmitterMetadata = commonData.submitterMetadata
  def submitter: LfPartyId = submitterMetadata.submitter
  def workflowId: Option[LfWorkflowId] = submitterMetadata.workflowId

  // Parties and participants
  override def informees: Set[LfPartyId] =
    view.contracts.stakeholders.all + commonData.submitterMetadata.submittingAdminParty
  def stakeholders: Stakeholders = commonData.stakeholders
  def confirmingParties: Set[LfPartyId] = commonData.confirmingParties

  def reassigningParticipants: Set[ParticipantId] = commonData.reassigningParticipants
  def isReassigningParticipant(participantId: ParticipantId): Boolean =
    reassigningParticipants.contains(participantId)

  // Contract
  def contracts: ContractsReassignmentBatch = view.contracts
}
