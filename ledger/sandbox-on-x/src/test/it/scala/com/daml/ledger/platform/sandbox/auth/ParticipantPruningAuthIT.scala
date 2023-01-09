// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.participant_pruning_service.{
  ParticipantPruningServiceGrpc,
  PruneRequest,
}

import scala.concurrent.Future

final class ParticipantPruningAuthIT extends AdminServiceCallAuthTests {

  override def serviceCallName: String = "ParticipantPruningService#Prune"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    stub(ParticipantPruningServiceGrpc.stub(channel), context.token)
      .prune(new PruneRequest(pruneUpTo = "000000000000"))

}
