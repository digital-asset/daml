// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore.api

import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

case class UserUpdate(
    id: Ref.UserId,
    primaryPartyUpdateO: Option[Option[Ref.Party]] = None,
    isDeactivatedUpdateO: Option[Boolean] = None,
    metadataUpdate: ObjectMetaUpdate,
)

case class ObjectMetaUpdate(
    resourceVersionO: Option[Long],
    annotationsUpdateO: Option[Map[String, String]],
) {
  def isNoUpdate: Boolean = annotationsUpdateO.isEmpty
}
object ObjectMetaUpdate {
  def empty: ObjectMetaUpdate = ObjectMetaUpdate(
    resourceVersionO = None,
    annotationsUpdateO = None,
  )
}

trait UserManagementStore {

  import UserManagementStore._

  // read access

  def getUserInfo(id: Ref.UserId, identityProviderId: Option[Ref.IdentityProviderId])(implicit
      loggingContext: LoggingContext
  ): Future[Result[UserInfo]]

  /** Always returns `maxResults` if possible, i.e. if a call to this method
    * returned fewer than `maxResults` users, then the next page (as of calling this method) was empty.
    */
  def listUsers(
      fromExcl: Option[Ref.UserId],
      maxResults: Int,
      identityProviderId: Option[Ref.IdentityProviderId],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[UsersPage]]

  // write access

  def createUser(user: User, rights: Set[UserRight])(implicit
      loggingContext: LoggingContext
  ): Future[Result[User]]

  def updateUser(userUpdate: UserUpdate)(implicit
      loggingContext: LoggingContext
  ): Future[Result[User]]

  def deleteUser(id: Ref.UserId, identityProviderId: Option[Ref.IdentityProviderId])(implicit
      loggingContext: LoggingContext
  ): Future[Result[Unit]]

  def grantRights(
      id: Ref.UserId,
      rights: Set[UserRight],
      identityProviderId: Option[Ref.IdentityProviderId],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[Set[UserRight]]]

  def revokeRights(
      id: Ref.UserId,
      rights: Set[UserRight],
      identityProviderId: Option[Ref.IdentityProviderId],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[Set[UserRight]]]

  // read helpers

  final def getUser(id: Ref.UserId, identityProviderId: Option[Ref.IdentityProviderId])(implicit
      loggingContext: LoggingContext
  ): Future[Result[User]] = {
    getUserInfo(id, identityProviderId).map(_.map(_.user))(ExecutionContext.parasitic)
  }

  final def listUserRights(id: Ref.UserId, identityProviderId: Option[Ref.IdentityProviderId])(
      implicit loggingContext: LoggingContext
  ): Future[Result[Set[UserRight]]] = {
    getUserInfo(id, identityProviderId).map(_.map(_.rights))(ExecutionContext.parasitic)
  }

}

object UserManagementStore {

  val DefaultParticipantAdminUserId = "participant_admin"

  type Result[T] = Either[Error, T]
  type Users = Seq[User]

  case class UsersPage(users: Seq[User]) {
    def lastUserIdOption: Option[Ref.UserId] = users.lastOption.map(_.id)
  }

  case class UserInfo(user: User, rights: Set[UserRight])

  sealed trait Error
  final case class UserNotFound(userId: Ref.UserId) extends Error
  final case class UserExists(userId: Ref.UserId) extends Error
  final case class TooManyUserRights(userId: Ref.UserId) extends Error
  final case class ConcurrentUserUpdate(userId: Ref.UserId) extends Error
  final case class MaxAnnotationsSizeExceeded(userId: Ref.UserId) extends Error
}
