// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain.{ObjectMeta, User, UserRight}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.api.UserManagementStore._
import com.daml.platform.localstore.api.{UserManagementStore, UserUpdate}
import com.daml.platform.localstore.utils.LocalAnnotationsUtils
import com.daml.platform.server.api.validation.ResourceAnnotationValidation

import scala.collection.mutable
import scala.concurrent.Future

//TODO DPP-1299 Include IdentityProviderAdmin
class InMemoryUserManagementStore(createAdmin: Boolean = true) extends UserManagementStore {
  import InMemoryUserManagementStore._

  // Underlying mutable map to keep track of UserInfo state.
  // Structured so we can use a ConcurrentHashMap (to more closely mimic a real implementation, where performance is key).
  // We synchronize on a private object (the mutable map), not the service (which could cause deadlocks).
  // (No need to mark state as volatile -- rely on synchronized to establish the JMM's happens-before relation.)
  private val state: mutable.TreeMap[Ref.UserId, InMemUserInfo] = mutable.TreeMap()
  if (createAdmin) {
    state.put(AdminUser.user.id, AdminUser)
  }

  override def getUserInfo(id: UserId, identityProviderId: Option[Ref.IdentityProviderId])(implicit
      loggingContext: LoggingContext
  ): Future[Result[UserManagementStore.UserInfo]] =
    withUser(id, identityProviderId)(info => Right(toDomainUserInfo(info)))

  override def createUser(user: User, rights: Set[UserRight])(implicit
      loggingContext: LoggingContext
  ): Future[Result[User]] =
    withoutUser(user.id) {
      for {
        _ <- validateAnnotationsSize(user.metadata.annotations, user.id)
      } yield {
        val userWithResourceVersion = {
          InMemUser(
            id = user.id,
            primaryParty = user.primaryParty,
            isDeactivated = user.isDeactivated,
            resourceVersion = 0,
            annotations = user.metadata.annotations,
            identityProviderId = user.identityProviderId,
          )
        }
        state.update(user.id, InMemUserInfo(userWithResourceVersion, rights))
        toDomainUser(userWithResourceVersion)
      }
    }

  override def updateUser(
      userUpdate: UserUpdate
  )(implicit loggingContext: LoggingContext): Future[Result[User]] = {
    withUser(userUpdate.id, None) {
      userInfo => // TODO DPP-1299  we should check if the user is allowed to edit
        val updatedPrimaryParty =
          userUpdate.primaryPartyUpdateO.getOrElse(userInfo.user.primaryParty)
        val updatedIsDeactivated =
          userUpdate.isDeactivatedUpdateO.getOrElse(userInfo.user.isDeactivated)
        val existingAnnotations = userInfo.user.annotations
        val updatedAnnotations =
          userUpdate.metadataUpdate.annotationsUpdateO.fold(existingAnnotations) { newAnnotations =>
            LocalAnnotationsUtils.calculateUpdatedAnnotations(
              newValue = newAnnotations,
              existing = existingAnnotations,
            )
          }
        val currentResourceVersion = userInfo.user.resourceVersion
        val newResourceVersionEither = userUpdate.metadataUpdate.resourceVersionO match {
          case None => Right(currentResourceVersion + 1)
          case Some(requestResourceVersion) =>
            if (requestResourceVersion == currentResourceVersion) {
              Right(currentResourceVersion + 1)
            } else {
              Left(UserManagementStore.ConcurrentUserUpdate(userUpdate.id))
            }
        }
        for {
          _ <- validateAnnotationsSize(updatedAnnotations, userUpdate.id)
          newResourceVersion <- newResourceVersionEither
        } yield {
          val updatedUserInfo = userInfo.copy(
            user = userInfo.user.copy(
              primaryParty = updatedPrimaryParty,
              isDeactivated = updatedIsDeactivated,
              resourceVersion = newResourceVersion,
              annotations = updatedAnnotations,
            )
          )
          state.update(userUpdate.id, updatedUserInfo)
          toDomainUser(updatedUserInfo.user)
        }
    }
  }

  override def deleteUser(
      id: Ref.UserId,
      identityProviderId: Option[Ref.IdentityProviderId],
  )(implicit loggingContext: LoggingContext): Future[Result[Unit]] =
    withUser(id, identityProviderId) { _ =>
      state.remove(id)
      Right(())
    }

  override def grantRights(
      id: Ref.UserId,
      granted: Set[UserRight],
      identityProviderId: Option[Ref.IdentityProviderId],
  )(implicit loggingContext: LoggingContext): Future[Result[Set[UserRight]]] =
    withUser(id, identityProviderId) { userInfo =>
      val newlyGranted = granted.diff(userInfo.rights) // faster than filter
      // we're not doing concurrent updates -- assert as backstop and a reminder to handle the collision case in the future
      assert(
        replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights ++ newlyGranted))
      )
      Right(newlyGranted)
    }

  override def revokeRights(
      id: Ref.UserId,
      revoked: Set[UserRight],
      identityProviderId: Option[Ref.IdentityProviderId],
  )(implicit loggingContext: LoggingContext): Future[Result[Set[UserRight]]] =
    withUser(id, identityProviderId) { userInfo =>
      val effectivelyRevoked = revoked.intersect(userInfo.rights) // faster than filter
      // we're not doing concurrent updates -- assert as backstop and a reminder to handle the collision case in the future
      assert(
        replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights -- effectivelyRevoked))
      )
      Right(effectivelyRevoked)
    }

  override def listUsers(
      fromExcl: Option[Ref.UserId],
      maxResults: Int,
      identityProviderId: Option[Ref.IdentityProviderId],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[UsersPage]] = {
    withState {
      val iter: Iterator[InMemUserInfo] = fromExcl match {
        case None => state.valuesIterator
        case Some(after) => state.valuesIteratorFrom(start = after).dropWhile(_.user.id == after)
      }
      val users: Seq[User] = iter
        .take(maxResults)
        .map(info => toDomainUser(info.user))
        .filter { item =>
          identityProviderId.nonEmpty && item.identityProviderId == identityProviderId || identityProviderId.isEmpty
        }
        .toSeq
      Right(UsersPage(users = users))
    }
  }

  def listAllUsers(): Future[List[User]] = withState(
    state.valuesIterator.map(info => toDomainUser(info.user)).toList
  )

  private def withState[T](t: => T): Future[T] =
    state.synchronized(
      Future.successful(t)
    )

  private def withUser[T](id: Ref.UserId, identityProviderId: Option[Ref.IdentityProviderId])(
      f: InMemUserInfo => Result[T]
  ): Future[Result[T]] =
    withState(
      state.get(id) match {
        case Some(user) if identityProviderId.isEmpty => f(user)
        case Some(user) if user.user.identityProviderId == identityProviderId => f(user)
        case _ => Left(UserNotFound(id))
      }
    )

  private def withoutUser[T](id: Ref.UserId)(t: => Result[T]): Future[Result[T]] =
    withState(
      state.get(id) match {
        case Some(_) => Left(UserExists(id))
        case None => t
      }
    )

  private def replaceInfo(oldInfo: InMemUserInfo, newInfo: InMemUserInfo): Boolean =
    state.synchronized {
      assert(
        oldInfo.user.id == newInfo.user.id,
        s"Replace info from if ${oldInfo.user.id} to ${newInfo.user.id} -> ${newInfo.rights}",
      )
      state.get(oldInfo.user.id) match {
        case Some(`oldInfo`) => state.update(newInfo.user.id, newInfo); true
        case _ => false
      }
    }

  private def validateAnnotationsSize(
      annotations: Map[String, String],
      userId: Ref.UserId,
  ): Result[Unit] = {
    if (!ResourceAnnotationValidation.isWithinMaxAnnotationsByteSize(annotations)) {
      Left(MaxAnnotationsSizeExceeded(userId))
    } else {
      Right(())
    }
  }

}

object InMemoryUserManagementStore {

  case class InMemUser(
      id: Ref.UserId,
      primaryParty: Option[Ref.Party],
      isDeactivated: Boolean = false,
      resourceVersion: Long,
      annotations: Map[String, String],
      identityProviderId: Option[Ref.IdentityProviderId],
  )
  case class InMemUserInfo(user: InMemUser, rights: Set[UserRight])

  def toDomainUserInfo(info: InMemUserInfo): UserInfo = {
    UserInfo(
      user = toDomainUser(info.user),
      rights = info.rights,
    )
  }

  def toDomainUser(user: InMemUser): User = {
    User(
      id = user.id,
      primaryParty = user.primaryParty,
      isDeactivated = user.isDeactivated,
      metadata = ObjectMeta(
        resourceVersionO = Some(user.resourceVersion),
        annotations = user.annotations,
      ),
    )
  }

  private val AdminUser = InMemUserInfo(
    user = InMemUser(
      id = Ref.UserId.assertFromString(UserManagementStore.DefaultParticipantAdminUserId),
      primaryParty = None,
      isDeactivated = false,
      resourceVersion = 0,
      annotations = Map.empty,
      identityProviderId = None,
    ),
    rights = Set(UserRight.ParticipantAdmin),
  )
}
