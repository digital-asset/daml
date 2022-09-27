// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  NoParties,
  Participant,
  Participants,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserRequest,
  CreateUserResponse,
  GetUserResponse,
  UpdateUserRequest,
  UpdateUserResponse,
  User,
}
import com.google.protobuf.field_mask.FieldMask
import com.daml.ledger.api.testtool.infrastructure.Assertions._

import scala.concurrent.{ExecutionContext, Future}

trait UserManagementServiceITUtils { self: UserManagementServiceIT =>

  def withFreshUser[T](
      primaryParty: String = "",
      isDeactivated: Boolean = false,
      annotations: Map[String, String] = Map.empty,
  )(
      f: User => Future[T]
  )(implicit ledger: ParticipantTestContext, ec: ExecutionContext): Future[T] = {
    val userId = ledger.nextUserId()
    val newUser = User(
      id = userId,
      primaryParty = primaryParty,
      isDeactivated = isDeactivated,
      metadata = Some(
        ObjectMeta(
          resourceVersion = "",
          annotations = annotations,
        )
      ),
    )
    for {
      create <- ledger.userManagement.createUser(CreateUserRequest(Some(newUser)))
      _ = assertEquals("withUser", unsetResourceVersion(create), CreateUserResponse(Some(newUser)))
      v <- f(create.user.get)
    } yield v
  }

  def newUser(
      id: String,
      isDeactivated: Boolean = false,
      primaryParty: String = "",
      annotations: Map[String, String] = Map.empty,
  ): User = User(
    id = id,
    isDeactivated = isDeactivated,
    primaryParty = primaryParty,
    metadata = Some(ObjectMeta(annotations = annotations)),
  )

  def updateRequest(
      id: String,
      isDeactivated: Boolean = false,
      primaryParty: String = "",
      resourceVersion: String = "",
      annotations: Map[String, String] = Map.empty,
      updatePaths: Seq[String],
  ): UpdateUserRequest =
    UpdateUserRequest(
      user = Some(
        User(
          id = id,
          isDeactivated = isDeactivated,
          primaryParty = primaryParty,
          metadata = Some(ObjectMeta(resourceVersion = resourceVersion, annotations = annotations)),
        )
      ),
      updateMask = Some(
        FieldMask(updatePaths)
      ),
    )

  def extractIsDeactivated(updateResp: UpdateUserResponse): Boolean =
    updateResp.getUser.isDeactivated

  def extractUpdatedPrimaryParty(updateResp: UpdateUserResponse): String =
    updateResp.getUser.primaryParty

  def extractUpdatedAnnotations(updateResp: UpdateUserResponse): Map[String, String] =
    updateResp.getUser.getMetadata.annotations

  def extractAnnotations(updateResp: CreateUserResponse): Map[String, String] =
    updateResp.user.get.metadata.get.annotations

  def unsetResourceVersion[T](t: T): T = {
    val t2: T = t match {
      case u: User => u.update(_.metadata.resourceVersion := "").asInstanceOf[T]
      case u: CreateUserResponse => u.update(_.user.metadata.resourceVersion := "").asInstanceOf[T]
      case u: UpdateUserResponse => u.update(_.user.metadata.resourceVersion := "").asInstanceOf[T]
      case u: GetUserResponse => u.update(_.user.metadata.resourceVersion := "").asInstanceOf[T]
      case other => sys.error(s"could not match $other")
    }
    t2
  }

  def userManagementTest(
      shortIdentifier: String,
      description: String,
      runConcurrently: Boolean = true,
      requiresUserAndPartyLocalMetadataExtensions: Boolean = false,
  )(
      body: ExecutionContext => ParticipantTestContext => Future[Unit]
  ): Unit = {
    test(
      shortIdentifier = shortIdentifier,
      description = description,
      allocate(NoParties),
      enabled = (features: Features) => {
        features.userManagement.supported &&
        (!requiresUserAndPartyLocalMetadataExtensions || features.userAndPartyLocalMetadataExtensions)
      },
      disabledReason = "requires user management feature",
      runConcurrently = runConcurrently,
    )(implicit ec => { case Participants(Participant(ledger)) =>
      body(ec)(ledger)
    })
  }

  def testWithFreshUser(
      shortIdentifier: String,
      description: String,
  )(
      primaryParty: String = "",
      isDeactivated: Boolean = false,
      annotations: Map[String, String] = Map.empty,
  )(
      body: ExecutionContext => ParticipantTestContext => User => Future[Unit]
  ): Unit = {
    userManagementTest(
      shortIdentifier = shortIdentifier,
      description = description,
      requiresUserAndPartyLocalMetadataExtensions = true,
    )(implicit ec => { implicit ledger =>
      withFreshUser(
        primaryParty = primaryParty,
        isDeactivated = isDeactivated,
        annotations = annotations,
      ) { user =>
        body(ec)(ledger)(user)
      }
    })
  }

  def assertUserNotFound(t: Throwable): Unit = {
    assertGrpcError(
      t = t,
      errorCode = LedgerApiErrors.Admin.UserManagement.UserNotFound,
      exceptionMessageSubstring = None,
    )
  }

  def assertUserAlreadyExists(
      t: Throwable
  ): Unit = {
    assertGrpcError(
      t = t,
      errorCode = LedgerApiErrors.Admin.UserManagement.UserAlreadyExists,
      exceptionMessageSubstring = None,
    )
  }

}
