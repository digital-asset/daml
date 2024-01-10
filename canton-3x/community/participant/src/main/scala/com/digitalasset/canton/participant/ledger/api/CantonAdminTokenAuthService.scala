// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.digitalasset.canton.crypto.RandomOps
import com.digitalasset.canton.ledger.api.auth.AuthService.AUTHORIZATION_KEY
import com.digitalasset.canton.ledger.api.auth.{AuthService, ClaimSet}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.HexString
import io.grpc.Metadata

import java.util.concurrent.{CompletableFuture, CompletionStage}

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
  * Ledger-Api server. However, if the Ledger-Api server is configured with JWT, they will fail.
  * But we can't expect that Canton obtains an oauth token from a third party service during startup.
  *
  * Therefore, we create on each startup a master token which is only ever shared internally.
  */
class CantonAdminTokenAuthService(adminToken: CantonAdminToken, parent: Seq[AuthService])
    extends AuthService {
  override def decodeMetadata(
      headers: Metadata
  )(implicit traceContext: TraceContext): CompletionStage[ClaimSet] = {
    val bearerTokenRegex = "Bearer (.*)".r
    val authToken = for {
      authKey <- Option(headers.get(AUTHORIZATION_KEY))
      token <- bearerTokenRegex.findFirstMatchIn(authKey).map(_.group(1))
      _ <- if (token == adminToken.secret) Some(()) else None
    } yield ()
    authToken
      .map(_ => wildcard)
      .getOrElse(if (parent.isEmpty) wildcard else decodeMetadataParent(headers))
  }

  private val wildcard = CompletableFuture.completedFuture(ClaimSet.Claims.Wildcard: ClaimSet)
  private val deny = CompletableFuture.completedFuture(ClaimSet.Unauthenticated: ClaimSet)

  private def decodeMetadataParent(
      headers: Metadata
  )(implicit traceContext: TraceContext): CompletionStage[ClaimSet] = {
    // iterate until we find one claim set which is not unauthenticated
    parent.foldLeft(deny) { case (acc, elem) =>
      acc.thenCompose { prevClaims =>
        if (prevClaims != ClaimSet.Unauthenticated)
          CompletableFuture.completedFuture(prevClaims)
        else
          elem.decodeMetadata(headers)
      }
    }
  }

}
