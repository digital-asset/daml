// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore.api

import com.daml.lf.data.Ref
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, User, UserRight}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLogging}

import scala.concurrent.Future

final case class UserUpdate(
    id: Ref.UserId,
    identityProviderId: IdentityProviderId,
    primaryPartyUpdateO: Option[Option[Ref.Party]] = None,
    isDeactivatedUpdateO: Option[Boolean] = None,
    metadataUpdate: ObjectMetaUpdate,
)

final case class ObjectMetaUpdate(
    resourceVersionO: Option[Long],
    annotationsUpdateO: Option[Map[String, String]],
)

object ObjectMetaUpdate {
  def empty: ObjectMetaUpdate = ObjectMetaUpdate(
    resourceVersionO = None,
    annotationsUpdateO = None,
  )
}

trait UserManagementStore { self: NamedLogging =>

  private val directEc = DirectExecutionContext(noTracingLogger)

  import UserManagementStore.*

  // read access

  def getUserInfo(id: Ref.UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[UserInfo]]

  /** Always returns `maxResults` if possible, i.e. if a call to this method
    * returned fewer than `maxResults` users, then the next page (as of calling this method) was empty.
    */
  def listUsers(
      fromExcl: Option[Ref.UserId],
      maxResults: Int,
      identityProviderId: IdentityProviderId,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[UsersPage]]

  // write access

  def createUser(user: User, rights: Set[UserRight])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[User]]

  def updateUser(userUpdate: UserUpdate)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[User]]

  def deleteUser(id: Ref.UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Unit]]

  def grantRights(id: Ref.UserId, rights: Set[UserRight], identityProviderId: IdentityProviderId)(
      implicit loggingContext: LoggingContextWithTrace
  ): Future[Result[Set[UserRight]]]

  def revokeRights(id: Ref.UserId, rights: Set[UserRight], identityProviderId: IdentityProviderId)(
      implicit loggingContext: LoggingContextWithTrace
  ): Future[Result[Set[UserRight]]]

  def updateUserIdp(
      id: Ref.UserId,
      sourceIdp: IdentityProviderId,
      targetIdp: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[User]]
  // read helpers

  final def getUser(id: Ref.UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[User]] =
    getUserInfo(id, identityProviderId).map(_.map(_.user))(directEc)

  final def listUserRights(id: Ref.UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Set[UserRight]]] =
    getUserInfo(id, identityProviderId).map(_.map(_.rights))(directEc)

}

object UserManagementStore {

  val DefaultParticipantAdminUserId = "participant_admin"

  type Result[T] = Either[Error, T]

  final case class UsersPage(users: Seq[User]) {
    def lastUserIdOption: Option[Ref.UserId] = users.lastOption.map(_.id)
  }

  final case class UserInfo(user: User, rights: Set[UserRight])

  sealed trait Error
  final case class UserNotFound(userId: Ref.UserId) extends Error
  final case class UserExists(userId: Ref.UserId) extends Error
  final case class TooManyUserRights(userId: Ref.UserId) extends Error
  final case class ConcurrentUserUpdate(userId: Ref.UserId) extends Error
  final case class MaxAnnotationsSizeExceeded(userId: Ref.UserId) extends Error
  final case class PermissionDenied(userId: Ref.UserId) extends Error

}
