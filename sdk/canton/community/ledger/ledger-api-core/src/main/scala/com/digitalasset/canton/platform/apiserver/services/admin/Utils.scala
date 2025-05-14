// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v2.admin as proto_admin
import com.digitalasset.canton.auth.AuthorizationChecksErrors
import com.digitalasset.canton.ledger.api.ObjectMeta
import com.digitalasset.canton.ledger.error.groups.UserManagementServiceErrors
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.logging.ErrorLoggingContext

import scala.concurrent.Future

object Utils {
  def toProtoObjectMeta(meta: ObjectMeta): proto_admin.object_meta.ObjectMeta =
    proto_admin.object_meta.ObjectMeta(
      resourceVersion = serializeResourceVersion(meta.resourceVersionO),
      annotations = meta.annotations,
    )

  private def serializeResourceVersion(resourceVersionO: Option[Long]): String =
    resourceVersionO.fold("")(_.toString)

  def handleResult[T](operation: String)(
      result: UserManagementStore.Result[T]
  )(implicit errorLogger: ErrorLoggingContext): Future[T] =
    result match {
      case Left(UserManagementStore.PermissionDenied(id)) =>
        Future.failed(
          AuthorizationChecksErrors.PermissionDenied
            .Reject(s"User $id belongs to another Identity Provider")
            .asGrpcError
        )
      case Left(UserManagementStore.UserNotFound(id)) =>
        Future.failed(
          UserManagementServiceErrors.UserNotFound
            .Reject(operation, id)
            .asGrpcError
        )

      case Left(UserManagementStore.UserDeletedWhileUpdating(id)) =>
        Future.failed(
          UserManagementServiceErrors.UserDeletedWhileUpdating
            .Reject(operation, id)
            .asGrpcError
        )

      case Left(UserManagementStore.UserExists(id)) =>
        Future.failed(
          UserManagementServiceErrors.UserAlreadyExists
            .Reject(operation, id)
            .asGrpcError
        )

      case Left(UserManagementStore.TooManyUserRights(id)) =>
        Future.failed(
          UserManagementServiceErrors.TooManyUserRights
            .Reject(operation, id: String)
            .asGrpcError
        )
      case Left(e: UserManagementStore.ConcurrentUserUpdate) =>
        Future.failed(
          UserManagementServiceErrors.ConcurrentUserUpdateDetected
            .Reject(userId = e.userId)
            .asGrpcError
        )

      case Left(e: UserManagementStore.MaxAnnotationsSizeExceeded) =>
        Future.failed(
          UserManagementServiceErrors.MaxUserAnnotationsSizeExceeded
            .Reject(userId = e.userId)
            .asGrpcError
        )

      case scala.util.Right(t) =>
        Future.successful(t)
    }
}
