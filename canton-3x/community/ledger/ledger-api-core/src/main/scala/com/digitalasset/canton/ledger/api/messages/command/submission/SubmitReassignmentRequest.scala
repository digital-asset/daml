// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.command.submission

import com.daml.lf.data.{Ref, Time}
import com.daml.lf.value.Value
import com.digitalasset.canton.protocol.{SourceDomainId, TargetDomainId}

final case class SubmitReassignmentRequest(
    submitter: Ref.Party,
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    submissionId: Ref.SubmissionId,
    workflowId: Option[Ref.WorkflowId],
    reassignmentCommand: Either[AssignCommand, UnassignCommand],
)

final case class UnassignCommand(
    sourceDomainId: SourceDomainId,
    targetDomainId: TargetDomainId,
    contractId: Value.ContractId,
)
final case class AssignCommand(
    sourceDomainId: SourceDomainId,
    targetDomainId: TargetDomainId,
    unassignId: Time.Timestamp,
)
