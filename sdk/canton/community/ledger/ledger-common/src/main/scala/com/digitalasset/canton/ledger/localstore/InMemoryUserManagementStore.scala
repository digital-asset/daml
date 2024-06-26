// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, ObjectMeta, User, UserRight}
import com.digitalasset.canton.ledger.api.validation.ResourceAnnotationValidator
import com.digitalasset.canton.ledger.localstore.api.{UserManagementStore, UserUpdate}
import com.digitalasset.canton.ledger.localstore.utils.LocalAnnotationsUtils
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.UserId

import scala.collection.mutable
import scala.concurrent.{Future, blocking}

import UserManagementStore.*

class InMemoryUserManagementStore(
    createAdmin: Boolean = true,
    val loggerFactory: NamedLoggerFactory,
) extends UserManagementStore
    with NamedLogging {
  import InMemoryUserManagementStore.*

  // Underlying mutable map to keep track of UserInfo state.
  // Structured so we can use a ConcurrentHashMap (to more closely mimic a real implementation, where performance is key).
  // We synchronize on a private object (the mutable map), not the service (which could cause deadlocks).
  // (No need to mark state as volatile -- rely on synchronized to establish the JMM's happens-before relation.)
  private val state: mutable.TreeMap[Ref.UserId, InMemUserInfo] = mutable.TreeMap()
  if (createAdmin) {
    state.put(AdminUser.user.id, AdminUser).discard
  }

  override def getUserInfo(id: UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[UserManagementStore.UserInfo]] =
    withUser(id, identityProviderId)(info => Right(toDomainUserInfo(info)))

  override def createUser(user: User, rights: Set[UserRight])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[User]] =
    withoutUser(user.id, user.identityProviderId) {
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
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[User]] = {
    withUser(userUpdate.id, userUpdate.identityProviderId) { userInfo =>
      val updatedPrimaryParty = userUpdate.primaryPartyUpdateO.getOrElse(userInfo.user.primaryParty)
      val updatedIsDeactivated =
        userUpdate.isDeactivatedUpdateO.getOrElse(userInfo.user.isDeactivated)
      val existingAnnotations = userInfo.user.annotations
      val identityProviderId = userInfo.user.identityProviderId
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
            identityProviderId = identityProviderId,
          )
        )
        state.update(userUpdate.id, updatedUserInfo)
        toDomainUser(updatedUserInfo.user)
      }
    }
  }

  override def deleteUser(
      id: Ref.UserId,
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[Unit]] =
    withUser(id, identityProviderId) { _ =>
      state.remove(id).discard
      Right(())
    }

  override def grantRights(
      id: Ref.UserId,
      granted: Set[UserRight],
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[Set[UserRight]]] =
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
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[Set[UserRight]]] =
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
      identityProviderId: IdentityProviderId,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[UsersPage]] = {
    withState {
      val iter: Iterator[InMemUserInfo] = fromExcl match {
        case None => state.valuesIterator
        case Some(after) => state.valuesIteratorFrom(start = after).dropWhile(_.user.id == after)
      }
      val users: Seq[User] = iter
        .filter(_.user.identityProviderId == identityProviderId)
        .take(maxResults)
        .map(info => toDomainUser(info.user))
        .toSeq
      Right(UsersPage(users = users))
    }
  }

  def listAllUsers(): Future[List[User]] = withState(
    state.valuesIterator.map(info => toDomainUser(info.user)).toList
  )

  override def updateUserIdp(
      id: Ref.UserId,
      sourceIdp: IdentityProviderId,
      targetIdp: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[User]] = {
    withUser(id = id, identityProviderId = sourceIdp) { info =>
      val user = info.user.copy(identityProviderId = targetIdp)
      val updated = info.copy(user = user)
      state.update(id, updated)
      Right(toDomainUser(updated.user))
    }
  }

  private def withState[T](t: => T): Future[T] =
    blocking(
      state.synchronized(
        Future.successful(t)
      )
    )

  private def withUser[T](id: Ref.UserId, identityProviderId: IdentityProviderId)(
      f: InMemUserInfo => Result[T]
  ): Future[Result[T]] =
    withState(
      state.get(id) match {
        case Some(user) if user.user.identityProviderId == identityProviderId => f(user)
        case Some(_) =>
          Left(PermissionDenied(id))
        case None =>
          Left(UserNotFound(id))
      }
    )

  private def withoutUser[T](id: Ref.UserId, identityProviderId: IdentityProviderId)(
      t: => Result[T]
  ): Future[Result[T]] =
    withState(
      state.get(id) match {
        case Some(user) if user.user.identityProviderId != identityProviderId =>
          Left(PermissionDenied(id))
        case Some(_) => Left(UserExists(id))
        case None => t
      }
    )

  private def replaceInfo(oldInfo: InMemUserInfo, newInfo: InMemUserInfo): Boolean =
    blocking(state.synchronized {
      assert(
        oldInfo.user.id == newInfo.user.id,
        s"Replace info from if ${oldInfo.user.id} to ${newInfo.user.id} -> ${newInfo.rights}",
      )
      state.get(oldInfo.user.id) match {
        case Some(`oldInfo`) => state.update(newInfo.user.id, newInfo); true
        case _ => false
      }
    })

  private def validateAnnotationsSize(
      annotations: Map[String, String],
      userId: Ref.UserId,
  ): Result[Unit] = {
    if (!ResourceAnnotationValidator.isWithinMaxAnnotationsByteSize(annotations)) {
      Left(MaxAnnotationsSizeExceeded(userId))
    } else {
      Right(())
    }
  }

}

object InMemoryUserManagementStore {

  final case class InMemUser(
      id: Ref.UserId,
      primaryParty: Option[Ref.Party],
      isDeactivated: Boolean = false,
      resourceVersion: Long,
      annotations: Map[String, String],
      identityProviderId: IdentityProviderId,
  )
  final case class InMemUserInfo(user: InMemUser, rights: Set[UserRight])

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
      identityProviderId = user.identityProviderId,
    )
  }

  private val AdminUser = InMemUserInfo(
    user = InMemUser(
      id = Ref.UserId.assertFromString(UserManagementStore.DefaultParticipantAdminUserId),
      primaryParty = None,
      isDeactivated = false,
      resourceVersion = 0,
      annotations = Map.empty,
      identityProviderId = IdentityProviderId.Default,
    ),
    rights = Set(UserRight.ParticipantAdmin),
  )
}
