// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication

import cats.implicits.*
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.AuthenticationError
import com.digitalasset.canton.sequencing.authentication.grpc.Constant
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import io.grpc.Metadata

object MemberAuthenticator {

  sealed trait VerifyTokenError {
    val message: String
    val errorCode: Option[String]
  }
  private object VerifyTokenError {
    final case class ParseError(message: String) extends VerifyTokenError {
      val errorCode: Option[String] = None
    }
    final case class AuthError(authenticationError: AuthenticationError) extends VerifyTokenError {
      val message: String = authenticationError.reason
      val errorCode: Option[String] = Some(authenticationError.code)
    }
  }

  final case class AuthenticationCredentials(
      token: AuthenticationToken,
      member: String,
      intendedSynchronizer: String,
  )

  def extractAuthenticationCredentials(headers: Metadata): Option[AuthenticationCredentials] =
    for {
      member <- Option(headers.get(Constant.MEMBER_ID_METADATA_KEY))
      intendedSynchronizer <- Option(headers.get(Constant.SYNCHRONIZER_ID_METADATA_KEY))
      token <- Option(headers.get(Constant.AUTH_TOKEN_METADATA_KEY))
    } yield AuthenticationCredentials(
      token,
      member,
      intendedSynchronizer,
    )

  /** Parses and authenticates the [[AuthenticationCredentials]] using the provided
    * [[MemberAuthenticationService]].
    */
  def authenticate(
      tokenValidator: MemberAuthenticationService,
      credentials: AuthenticationCredentials,
  ): Either[VerifyTokenError, StoredAuthenticationToken] =
    for {
      member <- parseMemberHeader(credentials.member)
      intendedSynchronizerId <- parseSynchronizerId(credentials.intendedSynchronizer)
      storedToken <- verifyToken(
        tokenValidator,
        credentials.token,
        member,
        intendedSynchronizerId,
      )
    } yield storedToken

  private def parseSynchronizerId(
      intendedSynchronizer: String
  ): Either[VerifyTokenError.ParseError, PhysicalSynchronizerId] =
    PhysicalSynchronizerId
      .fromProtoPrimitive(intendedSynchronizer, "AuthenticationCredentials.intendedSynchronizer")
      .leftMap(err => VerifyTokenError.ParseError(err.message))

  private def parseMemberHeader(memberId: String): Either[VerifyTokenError.ParseError, Member] =
    Member
      .fromProtoPrimitive(memberId, "memberId")
      .leftMap(err => s"Failed to deserialize member id: $err")
      .leftMap(VerifyTokenError.ParseError.apply)

  /** Checks the supplied authentication token for the member. */
  private def verifyToken(
      tokenValidator: MemberAuthenticationService,
      token: AuthenticationToken,
      member: Member,
      intendedSynchronizerId: PhysicalSynchronizerId,
  ): Either[VerifyTokenError, StoredAuthenticationToken] =
    tokenValidator
      .validateToken(intendedSynchronizerId, member, token)
      .leftMap[VerifyTokenError](VerifyTokenError.AuthError.apply)
}
