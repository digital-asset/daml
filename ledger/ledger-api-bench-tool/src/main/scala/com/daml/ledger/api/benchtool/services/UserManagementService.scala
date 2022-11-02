// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.utils.ErrorDetails
import com.daml.ledger.api.benchtool.AuthorizationHelper
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserRequest,
  GrantUserRightsRequest,
  User,
  UserManagementServiceGrpc,
  Right => UserRight,
}
import io.grpc.{Channel, StatusRuntimeException}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}

class UserManagementService(channel: Channel, authorizationToken: Option[String]) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val service: UserManagementServiceGrpc.UserManagementServiceStub =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(
      UserManagementServiceGrpc.stub(channel)
    )

  def createUserOrGrantRightsToExisting(
      userId: String,
      observerPartyNames: Seq[String],
      signatoryPartyName: String,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val rights = userRights(observerPartyNames, signatoryPartyName)
    createUser(userId, rights).recoverWith {
      case e: StatusRuntimeException
          if ErrorDetails.matches(e, LedgerApiErrors.Admin.UserManagement.UserAlreadyExists) =>
        logger.info(
          s"Benchmark user already exists (received error: ${e.getStatus.getDescription}) so granting rights the existing user."
        )
        grantUserRights(userId, rights)
    }
  }

  private def createUser(
      userId: String,
      rights: Seq[UserRight],
  )(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info(s"Creating a user: '$userId' with rights: ${rights.mkString(", ")}")
    service
      .createUser(
        CreateUserRequest(
          user = Some(User(id = userId, primaryParty = "")),
          rights = rights,
        )
      )
      .map(_ => ())
  }

  private def grantUserRights(
      userId: String,
      rights: Seq[UserRight],
  )(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info(s"Granting rights: ${rights.mkString(", ")} to the user: $userId")
    service
      .grantUserRights(
        GrantUserRightsRequest(
          userId = userId,
          rights = rights,
        )
      )
      .map(_ => ())
  }

  private def userRights(
      observerPartyNames: Seq[String],
      signatoryPartyName: String,
  ): Seq[UserRight] = {
    val actAs = UserRight(UserRight.Kind.CanActAs(UserRight.CanActAs(signatoryPartyName)))
    val readAs = observerPartyNames.map(observerPartyName =>
      UserRight(UserRight.Kind.CanReadAs(UserRight.CanReadAs(observerPartyName)))
    )
    actAs +: readAs
  }
}
