// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.AuthorizationHelper
import com.daml.platform.localstore.api.UserManagementStore
import io.grpc.Channel

import scala.concurrent.{ExecutionContext, Future}

class LedgerApiServices(
    channel: Channel,
    val ledgerId: String,
    userId: String,
    authorizationHelper: Option[AuthorizationHelper],
) {

  private val authorizationToken: Option[String] = authorizationHelper.map(_.tokenFor(userId))

  val activeContractsService =
    new ActiveContractsService(channel, ledgerId, authorizationToken = authorizationToken)
  val commandService = new CommandService(channel, authorizationToken = authorizationToken)
  val commandSubmissionService =
    new CommandSubmissionService(channel, authorizationToken = authorizationToken)
  val commandCompletionService =
    new CommandCompletionService(
      channel,
      ledgerId,
      userId = userId,
      authorizationToken = authorizationToken,
    )
  val packageManagementService =
    new PackageManagementService(channel, authorizationToken = authorizationToken)
  val packageService = new PackageService(channel, authorizationToken = authorizationToken)
  val partyManagementService =
    new PartyManagementService(channel, authorizationToken = authorizationToken)
  val transactionService =
    new TransactionService(channel, ledgerId, authorizationToken = authorizationToken)
  val userManagementService = new UserManagementService(channel, authorizationToken)
}

object LedgerApiServices {

  /** @return factory function for creating optionally authorized services for a given userId
    */
  def forChannel(
      authorizationHelper: Option[AuthorizationHelper],
      channel: Channel,
  )(implicit ec: ExecutionContext): Future[String => LedgerApiServices] = {
    val ledgerIdentityService: LedgerIdentityService =
      new LedgerIdentityService(
        channel = channel,
        authorizationToken =
          authorizationHelper.map(_.tokenFor(UserManagementStore.DefaultParticipantAdminUserId)),
      )
    ledgerIdentityService
      .fetchLedgerId()
      .map(ledgerId =>
        (userId: String) =>
          new LedgerApiServices(
            channel,
            ledgerId,
            userId = userId,
            authorizationHelper = authorizationHelper,
          )
      )
  }
}
