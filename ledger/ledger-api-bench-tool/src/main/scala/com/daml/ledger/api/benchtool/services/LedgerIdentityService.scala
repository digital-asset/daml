// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.AuthorizationHelper
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc,
}
import io.grpc.Channel
import org.slf4j.LoggerFactory

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class LedgerIdentityService(channel: Channel, authorizationToken: Option[String]) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(
      LedgerIdentityServiceGrpc.stub(channel)
    ): @nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
    )

  @nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*")
  def fetchLedgerId()(implicit ec: ExecutionContext): Future[String] =
    service
      .getLedgerIdentity(
        new GetLedgerIdentityRequest()
      )
      .transformWith {
        case Success(response) =>
          Future.successful {
            logger.info(s"Fetched ledger ID: ${response.ledgerId}")
            response.ledgerId
          }
        case Failure(exception) =>
          Future.failed {
            logger.error(
              s"Error during fetching of the ledger id. Details: ${exception.getLocalizedMessage}",
              exception,
            )
            exception
          }
      }

}
