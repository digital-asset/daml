// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.services

import com.digitalasset.canton.ledger.api.benchtool.AuthorizationHelper
import io.grpc.Channel

import scala.concurrent.{ExecutionContext, Future}

class LedgerApiServices(
    channel: Channel,
    userId: String,
    authorizationHelper: Option[AuthorizationHelper],
) {

  private val authorizationToken: Option[String] = authorizationHelper.map(_.tokenFor(userId))
  val commandService = new CommandService(channel, authorizationToken = authorizationToken)
  val commandSubmissionService =
    new CommandSubmissionService(channel, authorizationToken = authorizationToken)
  val commandCompletionService =
    new CommandCompletionService(
      channel,
      userId = userId,
      authorizationToken = authorizationToken,
    )
  val packageManagementService =
    new PackageManagementService(channel, authorizationToken = authorizationToken)
  val pruningService = new PruningService(channel, authorizationToken = authorizationToken)
  val packageService = new PackageService(channel, authorizationToken = authorizationToken)
  val partyManagementService =
    new PartyManagementService(channel, authorizationToken = authorizationToken)

  val stateService = new StateService(channel, authorizationToken = authorizationToken)
  val updateService = new UpdateService(channel, authorizationToken = authorizationToken)
  val userManagementService = new UserManagementService(channel, authorizationToken)
}

object LedgerApiServices {

  /** @return
    *   factory function for creating optionally authorized services for a given userId
    */
  def forChannel(
      authorizationHelper: Option[AuthorizationHelper],
      channel: Channel,
  )(implicit ec: ExecutionContext): Future[String => LedgerApiServices] = Future {
    (userId: String) =>
      new LedgerApiServices(
        channel,
        userId = userId,
        authorizationHelper = authorizationHelper,
      )
  }
}
