// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.object_meta

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.ExpectedErrorDescription
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.suites.v1_8.UserManagementServiceIT
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserRequest,
  GetUserRequest,
  User,
}

import scala.concurrent.{ExecutionContext, Future}

trait ObjectMetaTestsForUserManagementService extends ObjectMetaTests with ObjectMetaTestsBase {
  self: UserManagementServiceIT =>

  type Resource = User
  type ResourceId = String

  override private[object_meta] def getId(resource: Resource): ResourceId = resource.id

  override private[object_meta] def annotationsUpdateRequestFieldPath: String =
    "user.metadata.annotations"

  override private[object_meta] def annotationsUpdatePath: String =
    "metadata.annotations"

  override private[object_meta] def annotationsShortUpdatePath = "metadata"

  override private[object_meta] def resourceVersionUpdatePath = "metadata.resource_version"

  override private[object_meta] def resourceIdPath = "id"

  override private[object_meta] def extractAnnotations(resource: Resource): Map[String, String] =
    resource.getMetadata.annotations

  override private[object_meta] def extractMetadata(resource: Resource): ObjectMeta =
    resource.getMetadata

  override private[object_meta] def testWithFreshResource(
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

  override private[object_meta] def testWithoutResource(
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

  override private[object_meta] def createResourceWithAnnotations(
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

  override private[object_meta] def fetchNewestAnnotations(
      id: ResourceId
  )(implicit ec: ExecutionContext, ledger: ParticipantTestContext): Future[Map[String, String]] = {
    ledger.userManagement
      .getUser(GetUserRequest(userId = id))
      .map(_.user.get.getMetadata.annotations)
  }

  override private[object_meta] def update(
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

  override private[object_meta] def concurrentUserUpdateDetectedErrorDescription(
      id: ResourceId
  ): ExpectedErrorDescription = ExpectedErrorDescription(
    errorCode = LedgerApiErrors.Admin.UserManagement.ConcurrentUserUpdateDetected,
    exceptionMessageSubstring = Some(
      s"ABORTED: CONCURRENT_USER_UPDATE_DETECTED(2,0): Update operation for user '${id}' failed due to a concurrent update to the same user"
    ),
  )

  override private[object_meta] def invalidUpdateRequestErrorDescription(
      id: ResourceId,
      errorMessageSuffix: String,
  ): ExpectedErrorDescription = ExpectedErrorDescription(
    errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
    exceptionMessageSubstring = Some(
      s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${id}' failed due to: $errorMessageSuffix"
    ),
  )

}
