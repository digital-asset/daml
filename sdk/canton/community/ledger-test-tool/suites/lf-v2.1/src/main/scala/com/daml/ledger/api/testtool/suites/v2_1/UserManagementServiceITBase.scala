// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  NoParties,
  Participant,
  Participants,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, TestConstraints}
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.user_management_service.{
  CreateUserRequest,
  CreateUserResponse,
  GetUserResponse,
  UpdateUserRequest,
  UpdateUserResponse,
  User,
}
import com.digitalasset.canton.ledger.error.groups.AdminServiceErrors
import com.google.protobuf.field_mask.FieldMask

import scala.concurrent.{ExecutionContext, Future}

abstract class UserManagementServiceITBase extends LedgerTestSuite {

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
      identityProviderId = "",
    )
    for {
      create <- ledger.userManagement.createUser(CreateUserRequest(Some(newUser), Nil))
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
    metadata = Some(ObjectMeta(resourceVersion = "", annotations = annotations)),
    identityProviderId = "",
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
          identityProviderId = "",
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
      limitation: TestConstraints = TestConstraints.NoLimitations,
  )(
      body: ExecutionContext => ParticipantTestContext => Participants => Future[Unit]
  ): Unit =
    test(
      shortIdentifier = shortIdentifier,
      description = description,
      allocate(NoParties),
      enabled = (features: Features) => {
        features.userManagement.supported
      },
      disabledReason = "requires user management feature",
      runConcurrently = runConcurrently,
      limitation = limitation,
    )(implicit ec => { case p @ Participants(Participant(ledger, Seq())) =>
      body(ec)(ledger)(p)
    })

  def testWithFreshUser(
      shortIdentifier: String,
      description: String,
      limitation: TestConstraints = TestConstraints.NoLimitations,
  )(
      primaryParty: String = "",
      isDeactivated: Boolean = false,
      annotations: Map[String, String] = Map.empty,
  )(
      body: ExecutionContext => ParticipantTestContext => User => Future[Unit]
  ): Unit =
    userManagementTest(
      shortIdentifier = shortIdentifier,
      description = description,
      limitation = limitation,
    ) { implicit ec => implicit ledger => _ =>
      withFreshUser(
        primaryParty = primaryParty,
        isDeactivated = isDeactivated,
        annotations = annotations,
      ) { user =>
        body(ec)(ledger)(user)
      }
    }

  def assertUserNotFound(t: Throwable): Unit =
    assertGrpcError(
      t = t,
      errorCode = AdminServiceErrors.UserManagement.UserNotFound,
      exceptionMessageSubstring = None,
    )

  def assertUserAlreadyExists(
      t: Throwable
  ): Unit =
    assertGrpcError(
      t = t,
      errorCode = AdminServiceErrors.UserManagement.UserAlreadyExists,
      exceptionMessageSubstring = None,
    )

}
