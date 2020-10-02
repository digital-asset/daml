// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.refinements

import com.daml.ledger.api.refinements.ApiTypes.{CommandId, Party, WorkflowId}
import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.api.v1.trace_context.TraceContext

final case class CompositeCommand(
    commands: Seq[Command],
    party: Party,
    commandId: CommandId,
    workflowId: WorkflowId,
    traceContext: Option[TraceContext]
)
