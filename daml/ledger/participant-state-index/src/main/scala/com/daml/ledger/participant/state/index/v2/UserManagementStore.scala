// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

case class UserUpdate(
    id: Ref.UserId,
    primaryPartyUpdateO: Option[Option[Ref.Party]] = None,
    isDeactivatedUpdateO: Option[Boolean] = None,
    metadataUpdate: ObjectMetaUpdate,
) {
  def isNoUpdate: Boolean =
    primaryPartyUpdateO.isEmpty && isDeactivatedUpdateO.isEmpty && metadataUpdate.isNoUpdate
}

sealed trait AnnotationsUpdate {
  def annotations: Map[String, String]
}

object AnnotationsUpdate {
  final case class Merge private (nonEmptyAnnotations: Map[String, String])
      extends AnnotationsUpdate {
    def annotations: Map[String, String] = nonEmptyAnnotations
  }

  object Merge {
    def apply(annotations: Map[String, String]): Option[Merge] = {
      if (annotations.isEmpty) None
      else Some(new Merge(annotations))
    }

    def fromNonEmpty(annotations: Map[String, String]): Merge = {
      // TODO um-for-hub: Use error codes
      require(
        annotations.nonEmpty,
        "Unexpected new value for merge update: an empty map. Only non-empty maps are allowed",
      )
      new Merge(annotations)
    }
  }

  final case class Replace(annotations: Map[String, String]) extends AnnotationsUpdate
}

case class ObjectMetaUpdate(
    resourceVersionO: Option[Long],
    annotationsUpdateO: Option[AnnotationsUpdate],
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

  def getUserInfo(id: Ref.UserId)(implicit
      loggingContext: LoggingContext
  ): Future[Result[UserInfo]]

  /** Always returns `maxResults` if possible, i.e. if a call to this method
    * returned fewer than `maxResults` users, then the next page (as of calling this method) was empty.
    */
  def listUsers(fromExcl: Option[Ref.UserId], maxResults: Int)(implicit
      loggingContext: LoggingContext
  ): Future[Result[UsersPage]]

  // write access

  def createUser(user: User, rights: Set[UserRight])(implicit
      loggingContext: LoggingContext
  ): Future[Result[User]]

  // TODO um-for-hub major: Validate the size of update annotations is within max annotations size
  def updateUser(userUpdate: UserUpdate)(implicit
      loggingContext: LoggingContext
  ): Future[Result[User]]

  def deleteUser(id: Ref.UserId)(implicit loggingContext: LoggingContext): Future[Result[Unit]]

  // TODO um-for-hub major: Bump resource version number on rights change
  def grantRights(id: Ref.UserId, rights: Set[UserRight])(implicit
      loggingContext: LoggingContext
  ): Future[Result[Set[UserRight]]]

  // TODO um-for-hub major: Bump resource version number on rights change
  def revokeRights(id: Ref.UserId, rights: Set[UserRight])(implicit
      loggingContext: LoggingContext
  ): Future[Result[Set[UserRight]]]

  // read helpers

  final def getUser(id: Ref.UserId)(implicit
      loggingContext: LoggingContext
  ): Future[Result[User]] = {
    getUserInfo(id).map(_.map(_.user))(ExecutionContext.parasitic)
  }

  final def listUserRights(id: Ref.UserId)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Set[UserRight]]] = {
    getUserInfo(id).map(_.map(_.rights))(ExecutionContext.parasitic)
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

  sealed trait Error extends RuntimeException
  final case class UserNotFound(userId: Ref.UserId) extends Error
  final case class UserExists(userId: Ref.UserId) extends Error
  final case class TooManyUserRights(userId: Ref.UserId) extends Error
  final case class ConcurrentUserUpdate(party: Ref.UserId) extends Error
}
