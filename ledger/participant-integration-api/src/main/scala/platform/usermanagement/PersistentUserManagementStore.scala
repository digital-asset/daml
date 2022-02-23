// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import java.sql.Connection

import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore.{
  Result,
  TooManyUserRights,
  UserExists,
  UserInfo,
  UserNotFound,
  UsersPage,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Metrics}
import com.daml.platform.store.DbSupport
import com.daml.platform.store.backend.UserManagementStorageBackend
import com.daml.platform.usermanagement.PersistentUserManagementStore.TooManyUserRightsRuntimeException

import scala.concurrent.{ExecutionContext, Future}

object UserManagementConfig {

  val DefaultMaxCacheSize = 100
  val DefaultCacheExpiryAfterWriteInSeconds = 5
  val DefaultMaxUsersPageSize = 1000

  val MaxRightsPerUser = 1000

  def default(enabled: Boolean): UserManagementConfig = UserManagementConfig(
    enabled = enabled,
    maxCacheSize = DefaultMaxCacheSize,
    cacheExpiryAfterWriteInSeconds = DefaultCacheExpiryAfterWriteInSeconds,
    maxUsersPageSize = DefaultMaxUsersPageSize,
  )
}
final case class UserManagementConfig(
    enabled: Boolean,
    maxCacheSize: Int,
    cacheExpiryAfterWriteInSeconds: Int,
    maxUsersPageSize: Int,
)

object PersistentUserManagementStore {

  /** Intended to be thrown within a DB transaction to abort it.
    * The resulting failed future will get mapped to a successful future containing scala.util.Left
    */
  final case class TooManyUserRightsRuntimeException(userId: Ref.UserId) extends RuntimeException

  def cached(
      dbSupport: DbSupport,
      metrics: Metrics,
      timeProvider: TimeProvider,
      cacheExpiryAfterWriteInSeconds: Int,
      maxCacheSize: Int,
      maxRightsPerUser: Int,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): UserManagementStore = {
    new CachedUserManagementStore(
      delegate = new PersistentUserManagementStore(
        dbSupport = dbSupport,
        metrics = metrics,
        maxRightsPerUser = maxRightsPerUser,
        timeProvider = timeProvider,
      ),
      expiryAfterWriteInSeconds = cacheExpiryAfterWriteInSeconds,
      maximumCacheSize = maxCacheSize,
      metrics = metrics,
    )
  }
}

class PersistentUserManagementStore(
    dbSupport: DbSupport,
    metrics: Metrics,
    timeProvider: TimeProvider,
    maxRightsPerUser: Int,
) extends UserManagementStore {

  private val backend = dbSupport.storageBackendFactory.createUserManagementStorageBackend
  private val dbDispatcher = dbSupport.dbDispatcher

  private val logger = ContextualizedLogger.get(getClass)

  override def getUserInfo(id: UserId)(implicit
      loggingContext: LoggingContext
  ): Future[Result[UserInfo]] = {
    inTransaction(_.getUserInfo) { implicit connection =>
      withUser(id) { dbUser =>
        val rights = backend.getUserRights(internalId = dbUser.internalId)(connection)
        UserInfo(dbUser.domainUser, rights.map(_.domainRight))
      }
    }
  }

  override def createUser(
      user: domain.User,
      rights: Set[domain.UserRight],
  )(implicit loggingContext: LoggingContext): Future[Result[Unit]] = {
    inTransaction(_.createUser) { implicit connection: Connection =>
      withoutUser(user.id) {
        val now = epochMicroseconds()
        val internalId = backend.createUser(user, createdAt = now)(connection)
        rights.foreach(right =>
          backend.addUserRight(internalId = internalId, right = right, grantedAt = now)(
            connection
          )
        )
        if (backend.countUserRights(internalId)(connection) > maxRightsPerUser) {
          throw TooManyUserRightsRuntimeException(user.id)
        } else {
          ()
        }
        ()
      }
    }.map(tapSuccess { _ =>
      logger.info(
        s"Created new user: ${user} with ${rights.size} rights: ${rightsDigestText(rights)}"
      )
    })(scala.concurrent.ExecutionContext.parasitic)
  }

  override def deleteUser(
      id: UserId
  )(implicit loggingContext: LoggingContext): Future[Result[Unit]] = {
    inTransaction(_.deleteUser) { implicit connection =>
      if (!backend.deleteUser(id = id)(connection)) {
        Left(UserNotFound(userId = id))
      } else {
        Right(())
      }
    }.map(tapSuccess { _ =>
      logger.info(s"Deleted user with id: ${id}")
    })(scala.concurrent.ExecutionContext.parasitic)
  }

  override def grantRights(
      id: UserId,
      rights: Set[domain.UserRight],
  )(implicit loggingContext: LoggingContext): Future[Result[Set[domain.UserRight]]] = {
    inTransaction(_.grantRights) { implicit connection =>
      withUser(id = id) { user =>
        val now = epochMicroseconds()
        val addedRights = rights.filter { right =>
          if (!backend.userRightExists(internalId = user.internalId, right = right)(connection)) {
            backend.addUserRight(
              internalId = user.internalId,
              right = right,
              grantedAt = now,
            )(connection)
            true
          } else {
            false
          }
        }
        if (backend.countUserRights(user.internalId)(connection) > maxRightsPerUser) {
          throw TooManyUserRightsRuntimeException(user.domainUser.id)
        } else {
          addedRights
        }
      }
    }.map(tapSuccess { grantedRights =>
      logger.info(
        s"Granted ${grantedRights.size} user rights to user ${id}: ${rightsDigestText(grantedRights)}"
      )
    })(scala.concurrent.ExecutionContext.parasitic)
  }

  override def revokeRights(
      id: UserId,
      rights: Set[domain.UserRight],
  )(implicit loggingContext: LoggingContext): Future[Result[Set[domain.UserRight]]] = {
    inTransaction(_.revokeRights) { implicit connection =>
      withUser(id = id) { user =>
        val revokedRights = rights.filter { right =>
          backend.deleteUserRight(internalId = user.internalId, right = right)(connection)
        }
        revokedRights
      }
    }.map(tapSuccess { revokedRights =>
      logger.info(
        s"Revoked ${revokedRights.size} user rights from user ${id}: ${rightsDigestText(revokedRights)}"
      )
    })(scala.concurrent.ExecutionContext.parasitic)

  }

  override def listUsers(
      fromExcl: Option[Ref.UserId],
      maxResults: Int,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[UsersPage]] = {
    inTransaction(_.listUsers) { connection =>
      val users: Seq[domain.User] = fromExcl match {
        case None => backend.getUsersOrderedById(None, maxResults)(connection)
        case Some(fromExcl) => backend.getUsersOrderedById(Some(fromExcl), maxResults)(connection)
      }
      Right(UsersPage(users = users))
    }
  }

  private def inTransaction[T](
      dbMetric: metrics.daml.userManagement.type => DatabaseMetrics
  )(thunk: Connection => Result[T])(implicit loggingContext: LoggingContext): Future[Result[T]] = {
    dbDispatcher
      .executeSql(dbMetric(metrics.daml.userManagement))(thunk)
      .recover { case TooManyUserRightsRuntimeException(userId) =>
        Left(TooManyUserRights(userId))
      }(ExecutionContext.parasitic)
  }

  private def withUser[T](
      id: Ref.UserId
  )(f: UserManagementStorageBackend.DbUser => T)(implicit connection: Connection): Result[T] = {
    backend.getUser(id = id)(connection) match {
      case Some(user) => Right(f(user))
      case None => Left(UserNotFound(userId = id))
    }
  }

  private def withoutUser[T](
      id: Ref.UserId
  )(t: => T)(implicit connection: Connection): Result[T] = {
    backend.getUser(id = id)(connection) match {
      case Some(user) => Left(UserExists(userId = user.domainUser.id))
      case None => Right(t)
    }
  }

  private def tapSuccess[T](f: T => Unit)(r: Result[T]): Result[T] = {
    r.foreach(f)
    r
  }

  private def rightsDigestText(rights: Iterable[domain.UserRight]): String = {
    val closingBracket = if (rights.size > 5) ", ..." else ""
    rights.take(5).mkString("", ", ", closingBracket)
  }

  private def epochMicroseconds(): Long = {
    val now = timeProvider.getCurrentTime
    (now.getEpochSecond * 1000 * 1000) + (now.getNano / 1000)
  }
}
