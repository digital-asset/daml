// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication.grpc

import cats.data.EitherT
import cats.implicits.*
import com.digitalasset.canton.domain.sequencing.authentication.grpc.SequencerAuthenticationServerInterceptor.VerifyTokenError
import com.digitalasset.canton.domain.sequencing.authentication.{
  MemberAuthenticationService,
  StoredAuthenticationToken,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.{
  AuthenticationError,
  PassiveSequencer,
  TokenVerificationException,
}
import com.digitalasset.canton.sequencing.authentication.grpc.Constant
import com.digitalasset.canton.topology.{
  AuthenticatedMember,
  DomainId,
  Member,
  UnauthenticatedMemberId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.*

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class SequencerAuthenticationServerInterceptor(
    tokenValidator: MemberAuthenticationService,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ServerInterceptor
    with NamedLogging {

  override def interceptCall[ReqT, RespT](
      serverCall: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*

    (for {
      member <- Option(headers.get(Constant.MEMBER_ID_METADATA_KEY))
      intendedDomain <- Option(headers.get(Constant.DOMAIN_ID_METADATA_KEY))
    } yield (Option(headers.get(Constant.AUTH_TOKEN_METADATA_KEY)), member, intendedDomain))
      .fold[ServerCall.Listener[ReqT]] {
        logger.debug("Authentication headers are missing for member or domain-id")
        failVerification("Authentication headers are not set", serverCall, headers)
      } { case (tokenO, memberId, intendedDomain) =>
        new AsyncForwardingListener[ReqT] {
          // taking the context before an async callback happening (likely on another thread) is critical for maintaining
          // any original context values
          val originalContext = Context.current()

          val listenerET = for {
            member <- Member
              .fromProtoPrimitive(memberId, "memberId")
              .leftMap(err => s"Failed to deserialize member id: $err")
              .leftMap(VerifyTokenError.GeneralError)
              .toEitherT[Future]
            intendedDomainId <- UniqueIdentifier
              .fromProtoPrimitive_(intendedDomain)
              .map(DomainId(_))
              .leftMap(err => VerifyTokenError.GeneralError(err.message))
              .toEitherT[Future]
            storedTokenO <- member match {
              case _: UnauthenticatedMemberId =>
                EitherT.pure[Future, VerifyTokenError](None: Option[StoredAuthenticationToken])
              case authenticatedMember: AuthenticatedMember =>
                for {
                  token <- tokenO
                    .toRight(
                      VerifyTokenError.GeneralError("Authentication headers are missing for token")
                    )
                    .toEitherT[Future]
                  storedToken <- verifyToken(authenticatedMember, intendedDomainId, token).map(
                    _.some
                  )
                } yield storedToken
            }
          } yield {
            val contextWithAuthorizedMember = originalContext
              .withValue(IdentityContextHelper.storedAuthenticationTokenContextKey, storedTokenO)
              .withValue(IdentityContextHelper.storedMemberContextKey, Some(member))
            Contexts.interceptCall(contextWithAuthorizedMember, serverCall, headers, next)
          }

          listenerET.value.onComplete {
            case Success(Right(listener)) =>
              setNextListener(listener)
            case Success(Left(VerifyTokenError.AuthError(PassiveSequencer))) =>
              logger.debug("Authentication not possible with passive sequencer.")
              setNextListener(
                failVerification(
                  s"Verification failed for member $memberId: ${PassiveSequencer.reason}",
                  serverCall,
                  headers,
                  Some(PassiveSequencer.code),
                  Status.UNAVAILABLE,
                )
              )
            case Success(Left(err)) =>
              logger.debug(s"Authentication token verification failed: $err")
              setNextListener(
                failVerification(
                  s"Verification failed for member $memberId: ${err.message}",
                  serverCall,
                  headers,
                  err.errorCode,
                )
              )
            case Failure(ex) =>
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
      member: AuthenticatedMember,
      intendedDomain: DomainId,
      token: AuthenticationToken,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, VerifyTokenError, StoredAuthenticationToken] =
    tokenValidator
      .validateToken(intendedDomain, member, token)
      .leftMap[VerifyTokenError](VerifyTokenError.AuthError)

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
