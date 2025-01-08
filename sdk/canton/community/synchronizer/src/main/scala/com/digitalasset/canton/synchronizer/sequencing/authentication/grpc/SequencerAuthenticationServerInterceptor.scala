// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication.grpc

import cats.implicits.*
import com.digitalasset.canton.auth.AsyncForwardingListener
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.{
  AuthenticationError,
  TokenVerificationException,
}
import com.digitalasset.canton.sequencing.authentication.grpc.Constant
import com.digitalasset.canton.synchronizer.sequencing.authentication.{
  MemberAuthenticationService,
  StoredAuthenticationToken,
}
import com.digitalasset.canton.topology.{Member, SynchronizerId, UniqueIdentifier}
import io.grpc.*

import SequencerAuthenticationServerInterceptor.VerifyTokenError

class SequencerAuthenticationServerInterceptor(
    tokenValidator: MemberAuthenticationService,
    val loggerFactory: NamedLoggerFactory,
) extends ServerInterceptor
    with NamedLogging {

  override def interceptCall[ReqT, RespT](
      serverCall: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*

    (for {
      member <- Option(headers.get(Constant.MEMBER_ID_METADATA_KEY))
      intendedDomain <- Option(headers.get(Constant.SYNCHRONIZER_ID_METADATA_KEY))
    } yield (Option(headers.get(Constant.AUTH_TOKEN_METADATA_KEY)), member, intendedDomain))
      .fold[ServerCall.Listener[ReqT]] {
        logger.debug("Authentication headers are missing for member or synchronizer id")
        failVerification("Authentication headers are not set", serverCall, headers)
      } { case (tokenO, memberId, intendedDomain) =>
        new AsyncForwardingListener[ReqT] {
          // taking the context before an async callback happening (likely on another thread) is critical for maintaining
          // any original context values
          val originalContext = Context.current()

          try {
            val listenerE = for {
              member <- Member
                .fromProtoPrimitive(memberId, "memberId")
                .leftMap(err => s"Failed to deserialize member id: $err")
                .leftMap(VerifyTokenError.GeneralError.apply)
              intendedSynchronizerId <- UniqueIdentifier
                .fromProtoPrimitive_(intendedDomain)
                .map(SynchronizerId(_))
                .leftMap(err => VerifyTokenError.GeneralError(err.message))
              storedTokenO <-
                for {
                  token <- tokenO
                    .toRight[VerifyTokenError](
                      VerifyTokenError.GeneralError("Authentication headers are missing for token")
                    )
                  storedToken <- verifyToken(member, intendedSynchronizerId, token)
                } yield Some(storedToken)
            } yield {
              val contextWithAuthorizedMember = originalContext
                .withValue(IdentityContextHelper.storedAuthenticationTokenContextKey, storedTokenO)
                .withValue(IdentityContextHelper.storedMemberContextKey, Some(member))
              Contexts.interceptCall(contextWithAuthorizedMember, serverCall, headers, next)
            }

            listenerE match {
              case Right(listener) =>
                setNextListener(listener)
              case Left(err) =>
                logger.debug(s"Authentication token verification failed: $err")
                setNextListener(
                  failVerification(
                    s"Verification failed for member $memberId: ${err.message}",
                    serverCall,
                    headers,
                    err.errorCode,
                  )
                )
            }
          } catch {
            // To be on the safe side, in case `verifyToken` throws an exception
            case ex: Throwable =>
              logger.warn(s"Authentication token verification caused an unexpected exception", ex)
              setNextListener(
                failVerification(
                  s"Verification failed for participant $memberId with an internal error",
                  serverCall,
                  headers,
                  Some(TokenVerificationException(memberId).code),
                )
              )

          }
        }
      }
  }

  /** Checks the supplied authentication token for the member. */
  private def verifyToken(
      member: Member,
      intendedSynchronizerId: SynchronizerId,
      token: AuthenticationToken,
  ): Either[VerifyTokenError, StoredAuthenticationToken] =
    tokenValidator
      .validateToken(intendedSynchronizerId, member, token)
      .leftMap[VerifyTokenError](VerifyTokenError.AuthError.apply)

  private def failVerification[ReqT, RespT](
      msg: String,
      serverCall: ServerCall[ReqT, RespT],
      headers: Metadata,
      authErrorCode: Option[String] = None,
      status: Status = Status.UNAUTHENTICATED,
  ): ServerCall.Listener[ReqT] = {
    authErrorCode.foreach(code => headers.put(Constant.AUTHENTICATION_ERROR_CODE, code))
    serverCall.close(status.withDescription(msg), headers)
    new ServerCall.Listener[ReqT]() {}
  }
}

object SequencerAuthenticationServerInterceptor {
  sealed trait VerifyTokenError {
    val message: String
    val errorCode: Option[String]
  }
  object VerifyTokenError {
    final case class GeneralError(message: String) extends VerifyTokenError {
      val errorCode: Option[String] = None
    }
    final case class AuthError(authenticationError: AuthenticationError) extends VerifyTokenError {
      val message: String = authenticationError.reason
      val errorCode: Option[String] = Some(authenticationError.code)
    }
  }
}
