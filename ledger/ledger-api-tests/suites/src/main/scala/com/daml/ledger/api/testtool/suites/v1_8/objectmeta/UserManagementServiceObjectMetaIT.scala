// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.objectmeta

import com.daml.error.ErrorCode
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.suites.v1_8.UserManagementServiceITBase
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.user_management_service.{CreateUserRequest, GetUserRequest, User}
import com.daml.platform.error.definitions.LedgerApiErrors

import scala.concurrent.{ExecutionContext, Future}

class UserManagementServiceObjectMetaIT extends UserManagementServiceITBase with ObjectMetaTests {

  type Resource = User
  type ResourceId = String

  override private[objectmeta] def getId(resource: Resource): ResourceId = resource.id

  override private[objectmeta] def annotationsUpdateRequestFieldPath: String =
    "user.metadata.annotations"

  override private[objectmeta] def annotationsUpdatePath: String =
    "metadata.annotations"

  override private[objectmeta] def annotationsShortUpdatePath = "metadata"

  override private[objectmeta] def resourceVersionUpdatePath = "metadata.resource_version"

  override private[objectmeta] def resourceIdPath = "id"

  override private[objectmeta] def extractAnnotations(resource: Resource): Map[String, String] =
    resource.getMetadata.annotations

  override private[objectmeta] def extractMetadata(resource: Resource): ObjectMeta =
    resource.getMetadata

  override private[objectmeta] def testWithFreshResource(
      shortIdentifier: String,
      description: String,
  )(
      annotations: Map[String, String] = Map.empty
  )(
      body: ExecutionContext => ParticipantTestContext => Resource => Future[Unit]
  ): Unit = {
    userManagementTest(
      shortIdentifier = shortIdentifier,
      description = description,
      requiresUserAndPartyLocalMetadataExtensions = true,
    )(implicit ec => { ledger =>
      withFreshUser(
        annotations = annotations
      ) { user =>
        body(ec)(ledger)(user)
      }(ledger, ec)
    })
  }

  override private[objectmeta] def testWithoutResource(
      shortIdentifier: String,
      description: String,
  )(
      body: ExecutionContext => ParticipantTestContext => Future[Unit]
  ): Unit = {
    userManagementTest(
      shortIdentifier = shortIdentifier,
      description = description,
      requiresUserAndPartyLocalMetadataExtensions = true,
    )(implicit ec => { ledger =>
      body(ec)(ledger)
    })
  }

  override private[objectmeta] def createResourceWithAnnotations(
      annotations: Map[String, String]
  )(implicit ec: ExecutionContext, ledger: ParticipantTestContext): Future[Map[String, String]] = {
    val userId = ledger.nextUserId()
    val req = CreateUserRequest(
      user = Some(
        newUser(
          id = userId,
          annotations = annotations,
        )
      )
    )
    ledger.userManagement
      .createUser(req)
      .map(extractAnnotations)
  }

  override private[objectmeta] def fetchNewestAnnotations(
      id: ResourceId
  )(implicit ec: ExecutionContext, ledger: ParticipantTestContext): Future[Map[String, String]] = {
    ledger.userManagement
      .getUser(GetUserRequest(userId = id))
      .map(_.user.get.getMetadata.annotations)
  }

  override private[objectmeta] def update(
      id: ResourceId,
      annotations: Map[String, String],
      updatePaths: Seq[String],
      resourceVersion: String = "",
  )(implicit ec: ExecutionContext, ledger: ParticipantTestContext): Future[ObjectMeta] = {
    val req = updateRequest(
      id = id,
      annotations = annotations,
      resourceVersion = resourceVersion,
      updatePaths = updatePaths,
    )
    ledger.userManagement
      .updateUser(req)
      .map(_.getUser.getMetadata)
  }

  override private[objectmeta] def concurrentUserUpdateDetectedErrorCode: ErrorCode =
    LedgerApiErrors.Admin.UserManagement.ConcurrentUserUpdateDetected
  override private[objectmeta] def invalidUpdateRequestErrorCode: ErrorCode =
    LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest
}
