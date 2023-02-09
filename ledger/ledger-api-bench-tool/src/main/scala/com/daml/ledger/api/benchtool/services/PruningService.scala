// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.AuthorizationHelper
import com.daml.ledger.api.v1.admin.participant_pruning_service.{
  ParticipantPruningServiceGrpc,
  PruneRequest,
  PruneResponse,
}
import io.grpc.Channel

import scala.concurrent.Future

class PruningService(channel: Channel, authorizationToken: Option[String]) {
  private val service: ParticipantPruningServiceGrpc.ParticipantPruningServiceStub =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(
      ParticipantPruningServiceGrpc.stub(channel)
    )

  def prune(request: PruneRequest): Future[PruneResponse] = {
    service.prune(request)
  }
}
