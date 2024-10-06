// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.command.submission

import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.value.Value

final case class SubmitReassignmentRequest(
    submitter: Ref.Party,
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    submissionId: Ref.SubmissionId,
    workflowId: Option[Ref.WorkflowId],
    reassignmentCommand: Either[AssignCommand, UnassignCommand],
)

final case class UnassignCommand(
    sourceDomainId: Source[DomainId],
    targetDomainId: Target[DomainId],
    contractId: Value.ContractId,
)
final case class AssignCommand(
    sourceDomainId: Source[DomainId],
    targetDomainId: Target[DomainId],
    unassignId: Time.Timestamp,
)
