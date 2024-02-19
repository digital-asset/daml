// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, User}
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.validation.ResourceAnnotationValidator
import com.digitalasset.canton.ledger.localstore.CachedUserManagementStore
import com.digitalasset.canton.ledger.localstore.PersistentUserManagementStore.{
  ConcurrentUserUpdateDetectedRuntimeException,
  MaxAnnotationsSizeExceededException,
  TooManyUserRightsRuntimeException,
}
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore.*
import com.digitalasset.canton.ledger.localstore.api.{UserManagementStore, UserUpdate}
import com.digitalasset.canton.ledger.localstore.utils.LocalAnnotationsUtils
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.backend.localstore.UserManagementStorageBackend
import com.digitalasset.canton.tracing.TraceContext

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

class PersistentUserManagementStore(
    dbSupport: DbSupport,
    metrics: Metrics,
    timeProvider: TimeProvider,
    maxRightsPerUser: Int,
    val loggerFactory: NamedLoggerFactory,
) extends UserManagementStore
    with NamedLogging {

  private val directEc = DirectExecutionContext(noTracingLogger)

  private val backend = dbSupport.storageBackendFactory.createUserManagementStorageBackend
  private val dbDispatcher = dbSupport.dbDispatcher

  override def getUserInfo(id: UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[UserInfo]] =
    inTransaction(_.getUserInfo) { implicit connection =>
      withUser(id, identityProviderId) { dbUser =>
        val rights = backend.getUserRights(internalId = dbUser.internalId)(connection)
        val annotations = backend.getUserAnnotations(internalId = dbUser.internalId)(connection)
        val domainUser = toDomainUser(dbUser, annotations)
        UserInfo(domainUser, rights.map(_.domainRight))
      }
    }

  override def createUser(
      user: domain.User,
      rights: Set[domain.UserRight],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[User]] =
    inTransaction(_.createUser) { implicit connection: Connection =>
      withoutUser(user.id, user.identityProviderId) {
        val now = epochMicroseconds()
        if (
          !ResourceAnnotationValidator
            .isWithinMaxAnnotationsByteSize(user.metadata.annotations)
        ) {
          throw MaxAnnotationsSizeExceededException(userId = user.id)
        }
        val dbUser = UserManagementStorageBackend.DbUserPayload(
          id = user.id,
          primaryPartyO = user.primaryParty,
          identityProviderId = user.identityProviderId.toDb,
          isDeactivated = user.isDeactivated,
          resourceVersion = 0,
          createdAt = now,
        )
        val internalId = retryOnceMore(backend.createUser(user = dbUser)(connection))
        user.metadata.annotations.foreach { case (key, value) =>
          backend.addUserAnnotation(
            internalId = internalId,
            key = key,
            value = value,
            updatedAt = now,
          )(connection)
        }
        rights.foreach(right =>
          backend.addUserRight(internalId = internalId, right = right, grantedAt = now)(
            connection
          )
        )
        if (backend.countUserRights(internalId)(connection) > maxRightsPerUser) {
          throw TooManyUserRightsRuntimeException(user.id)
        }
        toDomainUser(
          dbUser = dbUser,
          annotations = user.metadata.annotations,
        )
      }
    }.map(tapSuccess { _ =>
      logger.info(
        s"Created new user: $user with " +
          (if (rights.size > 0)
             s"${rights.size} rights: ${rightsDigestText(rights)}"
           else "no rights") +
          s", ${loggingContext.serializeFiltered("submissionId")}."
      )
    })(directEc)

  override def updateUser(
      userUpdate: UserUpdate
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[User]] =
    inTransaction(_.updateUser) { implicit connection =>
      for {
        _ <- withUser(id = userUpdate.id, userUpdate.identityProviderId) { dbUser =>
          val now = epochMicroseconds()
          // Step 1: Update resource version
          // NOTE: We starts by writing to the 'resource_version' attribute
          //       of 'lapi_users' to effectively obtain an exclusive lock for
          //       updating this user for the rest of the transaction.
          val _ = userUpdate.metadataUpdate.resourceVersionO match {
            case Some(expectedResourceVersion) =>
              if (
                !backend.compareAndIncreaseResourceVersion(
                  internalId = dbUser.internalId,
                  expectedResourceVersion = expectedResourceVersion,
                )(connection)
              ) {
                throw ConcurrentUserUpdateDetectedRuntimeException(
                  userUpdate.id
                )
              }
            case None =>
              backend.increaseResourceVersion(
                internalId = dbUser.internalId
              )(connection)
          }
          // Step 2: Update annotations
          userUpdate.metadataUpdate.annotationsUpdateO.foreach { newAnnotations =>
            val existingAnnotations =
              backend.getUserAnnotations(dbUser.internalId)(connection)
            val updatedAnnotations = LocalAnnotationsUtils.calculateUpdatedAnnotations(
              newValue = newAnnotations,
              existing = existingAnnotations,
            )
            if (
              !ResourceAnnotationValidator
                .isWithinMaxAnnotationsByteSize(updatedAnnotations)
            ) {
              throw MaxAnnotationsSizeExceededException(userId = userUpdate.id)
            }
            backend.deleteUserAnnotations(internalId = dbUser.internalId)(connection)
            updatedAnnotations.iterator.foreach { case (key, value) =>
              backend.addUserAnnotation(
                internalId = dbUser.internalId,
                key = key,
                value = value,
                updatedAt = now,
              )(connection)
            }
          }
          // update is_deactivated
          userUpdate.isDeactivatedUpdateO.foreach { newValue =>
            backend.updateUserIsDeactivated(
              internalId = dbUser.internalId,
              isDeactivated = newValue,
            )(connection)
          }
          // update primary_party
          userUpdate.primaryPartyUpdateO.foreach { newValue =>
            backend.updateUserPrimaryParty(
              internalId = dbUser.internalId,
              primaryPartyO = newValue,
            )(connection)
          }
        }
        domainUser <- withUser(
          id = userUpdate.id,
          identityProviderId = userUpdate.identityProviderId,
        ) { dbUserAfterUpdates =>
          val annotations =
            backend.getUserAnnotations(internalId = dbUserAfterUpdates.internalId)(connection)
          toDomainUser(dbUser = dbUserAfterUpdates, annotations = annotations)
        }
      } yield domainUser
    }

  override def updateUserIdp(
      id: UserId,
      sourceIdp: IdentityProviderId,
      targetIdp: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[User]] =
    inTransaction(_.updateUserIdp) { implicit connection =>
      for {
        _ <- withUser(id = id, sourceIdp) { dbUser =>
          val _ = backend.updateUserIdp(
            internalId = dbUser.internalId,
            identityProviderId = targetIdp.toDb,
          )(connection)
        }
        domainUser <- withUser(
          id = id,
          identityProviderId = targetIdp,
        ) { dbUserAfterUpdates =>
          val annotations =
            backend.getUserAnnotations(internalId = dbUserAfterUpdates.internalId)(connection)
          toDomainUser(dbUser = dbUserAfterUpdates, annotations = annotations)
        }
      } yield domainUser
    }.map(tapSuccess { _ =>
      logger.info(s"Updated user $id idp from $sourceIdp to $targetIdp.")
    })(directEc)

  override def deleteUser(
      id: UserId,
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[Unit]] =
    inTransaction(_.deleteUser) { implicit connection =>
      withUser(id, identityProviderId) { _ =>
        backend.deleteUser(id = id)(connection)
      }.flatMap {
        case true => Right(())
        case false => Left(UserNotFound(userId = id))
      }
    }.map(tapSuccess { _ =>
      logger.info(
        s"Deleted user with id: $id, ${loggingContext.serializeFiltered("submissionId")}."
      )
    })(directEc)

  override def grantRights(
      id: UserId,
      rights: Set[domain.UserRight],
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[Set[domain.UserRight]]] =
    inTransaction(_.grantRights) { implicit connection =>
      withUser(id = id, identityProviderId) { user =>
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
          throw TooManyUserRightsRuntimeException(user.payload.id)
        } else {
          addedRights
        }
      }
    }.map(tapSuccess { grantedRights =>
      logger.info(
        s"Granted ${grantedRights.size} user rights to user $id: ${rightsDigestText(grantedRights)}, ${loggingContext
            .serializeFiltered("submissionId")}."
      )
    })(directEc)

  override def revokeRights(
      id: UserId,
      rights: Set[domain.UserRight],
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[Set[domain.UserRight]]] =
    inTransaction(_.revokeRights) { implicit connection =>
      withUser(id = id, identityProviderId) { user =>
        val revokedRights = rights.filter { right =>
          backend.deleteUserRight(internalId = user.internalId, right = right)(connection)
        }
        revokedRights
      }
    }.map(tapSuccess { revokedRights =>
      logger.info(
        s"Revoked ${revokedRights.size} user rights from user $id: ${rightsDigestText(revokedRights)}, ${loggingContext
            .serializeFiltered("submissionId")}."
      )
    })(directEc)

  override def listUsers(
      fromExcl: Option[Ref.UserId],
      maxResults: Int,
      identityProviderId: IdentityProviderId,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[UsersPage]] =
    inTransaction(_.listUsers) { connection =>
      val dbUsers = fromExcl match {
        case None =>
          backend.getUsersOrderedById(None, maxResults, identityProviderId)(connection)
        case Some(fromExcl) =>
          backend.getUsersOrderedById(Some(fromExcl), maxResults, identityProviderId)(
            connection
          )
      }
      val users = dbUsers.map { dbUser =>
        val annotations = backend.getUserAnnotations(dbUser.internalId)(connection)
        toDomainUser(dbUser = dbUser, annotations = annotations)
      }
      Right(UsersPage(users = users))
    }

  private def inTransaction[T](
      dbMetric: metrics.userManagement.type => DatabaseMetrics
  )(
      thunk: Connection => Result[T]
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[T]] = {
    def execute(): Future[Result[T]] =
      dbDispatcher.executeSql(dbMetric(metrics.userManagement))(thunk)
    implicit val ec: ExecutionContext = directEc
    execute()
      .recoverWith { case RetryOnceMoreException(cause) =>
        logger.debug("Retrying transaction to handle potential race", cause)
        execute()
      }
      .recover[Result[T]] {
        case TooManyUserRightsRuntimeException(userId) => Left(TooManyUserRights(userId))
        case ConcurrentUserUpdateDetectedRuntimeException(userId) =>
          Left(UserManagementStore.ConcurrentUserUpdate(userId))
        case MaxAnnotationsSizeExceededException(userId) =>
          Left(UserManagementStore.MaxAnnotationsSizeExceeded(userId))
      }
  }

  private def toDomainUser(
      dbUser: UserManagementStorageBackend.DbUserWithId,
      annotations: Map[String, String],
  ): domain.User =
    toDomainUser(
      dbUser = dbUser.payload,
      annotations = annotations,
    )

  private def toDomainUser(
      dbUser: UserManagementStorageBackend.DbUserPayload,
      annotations: Map[String, String],
  ): domain.User = {
    val payload = dbUser
    domain.User(
      id = payload.id,
      primaryParty = payload.primaryPartyO,
      isDeactivated = payload.isDeactivated,
      identityProviderId = IdentityProviderId.fromDb(payload.identityProviderId),
      metadata = domain.ObjectMeta(
        resourceVersionO = Some(payload.resourceVersion),
        annotations = annotations,
      ),
    )
  }

  private def withUser[T](
      id: Ref.UserId,
      identityProviderId: IdentityProviderId,
  )(
      f: UserManagementStorageBackend.DbUserWithId => T
  )(implicit connection: Connection): Result[T] =
    backend.getUser(id = id)(connection) match {
      case Some(user) if user.payload.identityProviderId == identityProviderId.toDb =>
        Right(f(user))
      case Some(_) => Left(PermissionDenied(userId = id))
      case None => Left(UserNotFound(userId = id))
    }

  private def withoutUser[T](
      id: Ref.UserId,
      identityProviderId: IdentityProviderId,
  )(t: => T)(implicit connection: Connection): Result[T] =
    backend.getUser(id = id)(connection) match {
      case Some(user) if user.payload.identityProviderId != identityProviderId.toDb =>
        Left(PermissionDenied(userId = id))
      case Some(user) =>
        Left(UserExists(userId = user.payload.id))
      case None => Right(t)
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

  private def retryOnceMore[T](body: => T): T =
    try {
      body
    } catch {
      case t: Throwable => throw RetryOnceMoreException(t)
    }
}

object PersistentUserManagementStore {

  /** Intended to be thrown within a DB transaction to abort it.
    * The resulting failed future will get mapped to a successful future containing scala.util.Left
    */
  final case class TooManyUserRightsRuntimeException(userId: Ref.UserId) extends RuntimeException

  final case class ConcurrentUserUpdateDetectedRuntimeException(userId: Ref.UserId)
      extends RuntimeException

  final case class MaxAnnotationsSizeExceededException(userId: Ref.UserId) extends RuntimeException

  def cached(
      dbSupport: DbSupport,
      metrics: Metrics,
      timeProvider: TimeProvider,
      cacheExpiryAfterWriteInSeconds: Int,
      maxCacheSize: Int,
      maxRightsPerUser: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): UserManagementStore =
    new CachedUserManagementStore(
      delegate = new PersistentUserManagementStore(
        dbSupport = dbSupport,
        metrics = metrics,
        maxRightsPerUser = maxRightsPerUser,
        timeProvider = timeProvider,
        loggerFactory = loggerFactory,
      ),
      expiryAfterWriteInSeconds = cacheExpiryAfterWriteInSeconds,
      maximumCacheSize = maxCacheSize,
      metrics = metrics,
      loggerFactory = loggerFactory,
    )(executionContext, LoggingContextWithTrace(loggerFactory))
}

final case class RetryOnceMoreException(underlying: Throwable) extends RuntimeException
