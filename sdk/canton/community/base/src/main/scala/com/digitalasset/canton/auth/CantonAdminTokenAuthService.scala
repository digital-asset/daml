// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.config.AdminTokenConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.Party

import scala.concurrent.Future

/** AuthService interceptor used for internal canton services
  *
  * Internal Canton services such as the PingService or the DarService require access to the
  * Ledger-Api server. However, if the Ledger-Api server is configured with JWT, they will fail. But
  * we can't expect that Canton obtains an oauth token from a third party service during startup.
  *
  * Therefore, we create on each startup a master token which is only ever shared internally.
  */
class CantonAdminTokenAuthService(
    adminTokenDispenser: CantonAdminTokenDispenser,
    adminParty: Option[Party],
    adminTokenConfig: AdminTokenConfig,
) extends AuthService {
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
      .fold(deny)(_ => permit)
  }

  private val partyClaim =
    if (adminTokenConfig.actAsAnyPartyClaim)
      List[Claim](ClaimActAsAnyParty)
    else
      adminParty.map(ClaimActAsParty.apply).toList

  private val adminClaim = Option.when(adminTokenConfig.adminClaim)(ClaimAdmin).toList

  private val permit = Future.successful(
    ClaimSet.Claims.Empty.copy(
      claims = List[Claim](ClaimPublic) ++ partyClaim ++ adminClaim
    )
  )

  private val deny = Future.successful(ClaimSet.Unauthenticated: ClaimSet)
}
