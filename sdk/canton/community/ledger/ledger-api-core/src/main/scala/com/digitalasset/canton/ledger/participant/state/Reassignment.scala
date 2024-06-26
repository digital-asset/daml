// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{SourceDomainId, TargetDomainId}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value

sealed trait Reassignment {
  def kind: String
}

object Reassignment {

  /** Represent the update of unassigning a contract from a domain.
    *
    * @param contractId            Contract ID of the underlying contract.
    * @param templateId            Template ID of the underlying contract.
    * @param packageName           Package name of the underlying contract's template.
    * @param stakeholders          Stakeholders of the underlying contract.
    * @param assignmentExclusivity Before this time (measured on the target domain), only the submitter
    *                              of the unassignment can initiate the assignment. Defined for
    *                              reassigning participants.
    */
  final case class Unassign(
      contractId: Value.ContractId,
      templateId: Ref.Identifier,
      packageName: Ref.PackageName,
      stakeholders: List[Ref.Party],
      assignmentExclusivity: Option[Timestamp],
  ) extends Reassignment {
    override def kind: String = "unassignment"
  }

  /** Represents the update of assigning a contract to a domain.
    *
    * @param ledgerEffectiveTime The ledger time of the creation of the underlying contract.
    * @param createNode          The details of the creation of the underlying contract.
    * @param contractMetadata    The metadata provided at creation of the underlying contract.
    */
  final case class Assign(
      ledgerEffectiveTime: Timestamp,
      createNode: Node.Create,
      contractMetadata: Bytes,
  ) extends Reassignment {
    override def kind: String = "assignment"
  }
}

/** The common information for all reassigments.
  * Except from the hosted and reassigning stakeholders, all fields are the same for
  * reassign and assign updates, which belong to the same reassignment.
  *
  * @param sourceDomain            The domain ID from the contract is unassigned.
  * @param targetDomain            The domain ID to the contract is assigned.
  * @param submitter               Submitter of the command, unless the operation is performed offline.
  * @param reassignmentCounter     This counter is strictly increasing with each reassignment
  *                                for one contract.
  * @param hostedStakeholders      The stakeholders of the related contract which are hosted on this
  *                                participant at the time of the corresponding request.
  * @param unassignId              The ID of the unassign event. This should be used for the assign
  *                                command.
  */
final case class ReassignmentInfo(
    sourceDomain: SourceDomainId,
    targetDomain: TargetDomainId,
    submitter: Option[Ref.Party],
    reassignmentCounter: Long,
    hostedStakeholders: List[Ref.Party],
    unassignId: CantonTimestamp,
)
