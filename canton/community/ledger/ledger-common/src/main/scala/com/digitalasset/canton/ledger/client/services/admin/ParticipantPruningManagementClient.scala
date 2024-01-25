// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.admin

import com.daml.ledger.api.v1.admin.participant_pruning_service.ParticipantPruningServiceGrpc.ParticipantPruningServiceStub
import com.daml.ledger.api.v1.admin.participant_pruning_service.{PruneRequest, PruneResponse}
import com.digitalasset.canton.ledger.client.LedgerClient

import scala.concurrent.Future

object ParticipantPruningManagementClient {

  private def pruneRequest(pruneUpTo: String, submissionId: Option[String]) =
    PruneRequest(pruneUpTo = pruneUpTo, submissionId = submissionId.getOrElse(""))

}

final class ParticipantPruningManagementClient(service: ParticipantPruningServiceStub) {

  def prune(
      pruneUpTo: String,
      token: Option[String] = None,
      submissionId: Option[String] = None,
  ): Future[PruneResponse] =
    LedgerClient
      .stub(service, token)
      .prune(ParticipantPruningManagementClient.pruneRequest(pruneUpTo, submissionId))

}
