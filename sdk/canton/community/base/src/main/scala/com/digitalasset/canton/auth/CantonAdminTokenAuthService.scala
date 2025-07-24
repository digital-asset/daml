// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.crypto.RandomOps
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.HexString
import com.digitalasset.canton.util.TimingSafeComparisonUtil.constantTimeEquals

import java.time.{Clock, Duration, Instant}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

final case class CantonAdminToken(secret: String)
object CantonAdminToken {
  def create(randomOps: RandomOps): CantonAdminToken = {
    val secret = HexString.toHexString(randomOps.generateRandomByteString(64))
    new CantonAdminToken(secret)
  }
}

/** AuthService interceptor used for internal canton services
  *
  * Internal Canton services such as the PingService or the DarService require access to the
  * Ledger-Api server. However, if the Ledger-Api server is configured with JWT, they will fail. But
  * we can't expect that Canton obtains an oauth token from a third party service during startup.
  *
  * Therefore, we create on each startup a master token which is only ever shared internally.
  */
class CantonAdminTokenAuthService(adminTokenDispenser: CantonAdminTokenDispenser)
    extends AuthService {
  override def decodeToken(
      authToken: Option[String],
      serviceName: String,
  )(implicit traceContext: TraceContext): Future[ClaimSet] = {
    val bearerTokenRegex = "Bearer (.*)".r
    val authTokenOpt = for {
      authKey <- authToken
      token <- bearerTokenRegex.findFirstMatchIn(authKey).map(_.group(1))
      _ <- if (adminTokenDispenser.checkToken(token)) Some(()) else None
    } yield ()
    authTokenOpt
      .fold(deny)(_ => wildcard)
  }

  private val wildcard = Future.successful(ClaimSet.Claims.Wildcard: ClaimSet)
  private val deny = Future.successful(ClaimSet.Unauthenticated: ClaimSet)
}

/** Token dispenser for CantonAdminToken
  *
  * This dispenser generates a new token every `tokenDuration` and rotates it every
  * `rotationInterval` (`tokenDuration` / 2). It keeps track of the current and previous tokens,
  * allowing for a grace period during which both tokens are valid. If a fixed token is provided, it
  * can be used without time validation.
  *
  * The fixed token is only used for testing purposes, and should not be used in production.
  */
class CantonAdminTokenDispenser(
    randomOps: RandomOps,
    tokenDuration: Duration,
    fixedToken: Option[String] = None,
    clock: Clock = Clock.systemUTC(),
) {
  // Token rotation interval is half the token validity duration
  private val rotationInterval = tokenDuration.dividedBy(2)

  private case class Token(token: CantonAdminToken, validFrom: Instant)

  private case class TokenState(
      currentToken: Token,
      previousToken: Token,
  )

  private val initialToken = Token(generateToken, Instant.now(clock))
  private val state = new AtomicReference(
    TokenState(initialToken, initialToken)
  )

  private def generateToken: CantonAdminToken = CantonAdminToken.create(randomOps)

  private def rotateIfNeeded(now: Instant): TokenState =
    state.updateAndGet { s =>
      if (now.isAfter(s.currentToken.validFrom.plus(rotationInterval))) {
        TokenState(Token(generateToken, now), s.currentToken)
      } else s
    }

  def getCurrentToken: CantonAdminToken = {
    val now = Instant.now(clock)
    rotateIfNeeded(now).currentToken.token
  }

  def checkToken(token: String): Boolean = {
    val now = Instant.now(clock)
    val s = state.get()

    // Check if the provided token matches either the current or the previous token, and if they are still valid
    val isCurrentValid = now.isBefore(
      s.currentToken.validFrom.plus(tokenDuration)
    ) && constantTimeEquals(s.currentToken.token.secret, token)
    lazy val isPreviousValid = now.isBefore(
      s.previousToken.validFrom.plus(tokenDuration)
    ) && constantTimeEquals(s.previousToken.token.secret, token)

    // If a fixed token is provided, check against it without time validation
    lazy val isFixedValid = fixedToken.exists(constantTimeEquals(_, token))

    isCurrentValid || isPreviousValid || isFixedValid
  }
}
