// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication.grpc

import com.digitalasset.canton.auth.AsyncForwardingListener
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.TokenVerificationException
import com.digitalasset.canton.sequencing.authentication.grpc.Constant
import com.digitalasset.canton.synchronizer.sequencing.authentication.MemberAuthenticationService
import com.digitalasset.canton.synchronizer.sequencing.authentication.MemberAuthenticator.{
  AuthenticationCredentials,
  authenticate,
}
import com.digitalasset.canton.synchronizer.sequencing.authentication.grpc.SequencerAuthenticationServerInterceptor.{
  InterceptorListener,
  extractAuthenticationHeaders,
  failVerification,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.*

class SequencerAuthenticationServerInterceptor(
    tokenValidator: MemberAuthenticationService,
    override val loggerFactory: NamedLoggerFactory,
) extends ServerInterceptor
    with NamedLogging {

  override def interceptCall[ReqT, RespT](
      serverCall: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*
    extractAuthenticationHeaders(headers)
      .fold[ServerCall.Listener[ReqT]] {
        logger.debug("Authentication headers are missing for member or synchronizer id")
        failVerification("Authentication headers are not set", serverCall, headers)
      } {
        new InterceptorListener(
          tokenValidator,
          serverCall,
          headers,
          next,
          _,
          loggerFactory,
        )
      }
  }
}

object SequencerAuthenticationServerInterceptor {

  private class InterceptorListener[ReqT, ResT](
      tokenValidator: MemberAuthenticationService,
      serverCall: ServerCall[ReqT, ResT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, ResT],
      authenticationCredentials: AuthenticationCredentials,
      override val loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext)
      extends AsyncForwardingListener[ReqT]
      with NamedLogging {

    // taking the context before an async callback happening (likely on another thread) is critical for maintaining
    // any original context values
    private val originalContext = Context.current()

    private val memberId = authenticationCredentials.member

    try {
      val listenerE =
        authenticate(tokenValidator, authenticationCredentials).map { token =>
          val contextWithAuthorizedMember = originalContext
            .withValue(
              IdentityContextHelper.storedAuthenticationTokenContextKey,
              Some(token),
            )
            .withValue(IdentityContextHelper.storedMemberContextKey, Some(token.member))
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

  private def extractAuthenticationHeaders(headers: Metadata): Option[AuthenticationCredentials] =
    for {
      member <- Option(headers.get(Constant.MEMBER_ID_METADATA_KEY))
      intendedSynchronizer <- Option(headers.get(Constant.SYNCHRONIZER_ID_METADATA_KEY))
      token <- Option(headers.get(Constant.AUTH_TOKEN_METADATA_KEY))
    } yield AuthenticationCredentials(
      token,
      member,
      intendedSynchronizer,
    )
}
