// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.admin

import com.daml.ledger.api.v2.admin.participant_pruning_service.ParticipantPruningServiceGrpc.ParticipantPruningServiceStub
import com.daml.ledger.api.v2.admin.participant_pruning_service.{PruneRequest, PruneResponse}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

object ParticipantPruningManagementClient {

  private def pruneRequest(pruneUpTo: Long, submissionId: Option[String]) =
    PruneRequest(pruneUpTo = pruneUpTo, submissionId = submissionId.getOrElse(""))

}

final class ParticipantPruningManagementClient(service: ParticipantPruningServiceStub) {

  def prune(
      pruneUpTo: Long,
      token: Option[String] = None,
      submissionId: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[PruneResponse] =
    LedgerClient
      .stubWithTracing(service, token)
      .prune(ParticipantPruningManagementClient.pruneRequest(pruneUpTo, submissionId))

}
