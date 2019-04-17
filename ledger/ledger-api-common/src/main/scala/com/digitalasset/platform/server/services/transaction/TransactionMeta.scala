// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.ledger.api.domain._
import com.digitalasset.ledger.api.v1.trace_context.TraceContext

/** Contains information that is necessary to reconstruct API transactions beside the daml-lf transaction. */
final case class TransactionMeta(
    transactionId: TransactionId,
    commandId: Option[CommandId],
    applicationId: Option[ApplicationId],
    submitter: Option[Party],
    workflowId: WorkflowId,
    effectiveAt: Instant,
    traceContext: Option[TraceContext]
)
