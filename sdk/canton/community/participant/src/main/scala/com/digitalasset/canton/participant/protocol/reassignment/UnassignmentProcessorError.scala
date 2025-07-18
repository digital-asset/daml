// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.store.ActiveContractStore.Status
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.PhysicalSynchronizerId

trait UnassignmentProcessorError extends ReassignmentProcessorError

object UnassignmentProcessorError {

  final case class UnexpectedSynchronizer(
      reassignmentId: ReassignmentId,
      receivedOn: PhysicalSynchronizerId,
  ) extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign `$reassignmentId`: received reassignment on $receivedOn"
  }

  final case class TargetSynchronizerIsSourceSynchronizer(
      synchronizerId: PhysicalSynchronizerId,
      contractIds: Seq[LfContractId],
  ) extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign contracts `$contractIds`: source and target synchronizers are the same"
  }

  final case class UnknownContract(contractId: LfContractId) extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign contract `$contractId`: unknown contract"
  }

  final case class DeactivatedContract(contractId: LfContractId, status: Status)
      extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign contract `$contractId` because it's not active. Current status $status"
  }

  final case object ReassignmentCounterOverflow extends ReassignmentProcessorError {
    override def message: String = "Reassignment counter overflow"
  }

  final case class AutomaticAssignmentError(message: String) extends UnassignmentProcessorError
}
