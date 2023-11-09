// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine.script.ledgerinteraction

import com.daml.ledger.client.LedgerClient
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.AdminLedgerClient
import com.daml.lf.speedy.{TraceLog, WarningLog}
import org.apache.pekko.http.scaladsl.model._
import com.daml.jwt.domain.Jwt
import com.daml.lf.typesig.EnvironmentSignature
import org.apache.pekko.actor.ActorSystem

// Hopefully remove after json client application id check
import com.daml.jwt.JwtDecoder
import com.daml.ledger.api.auth.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  CustomDamlJWTPayload,
  StandardJWTPayload,
}
import scalaz.{-\/, \/-}
import scala.util.{Failure, Success}

// Ledger clients before implementation is chosen
sealed trait ScriptLedgerClient extends Product with Serializable

final case class GrpcLedgerClient(
    grpcClient: LedgerClient,
    val applicationId: ApplicationId,
    val grpcAdminClient: Option[AdminLedgerClient] = None,
) extends ScriptLedgerClient

final case class JsonLedgerClient(
    uri: Uri,
    token: Jwt,
    envIface: EnvironmentSignature,
    actorSystem: ActorSystem,
) extends ScriptLedgerClient {
  // TODO: Sadly we check the application Id in the runner, so we need this logic here
  // See if theres a way to avoid that, or avoid the duplication
  private val decodedJwt = JwtDecoder.decode(token) match {
    case -\/(e) => throw new IllegalArgumentException(e.toString)
    case \/-(a) => a
  }
  private[script] val tokenPayload: AuthServiceJWTPayload =
    AuthServiceJWTCodec.readFromString(decodedJwt.payload) match {
      case Failure(e) => throw e
      case Success(s) => s
    }

  implicit val system: ActorSystem = actorSystem
  implicit val executionContext: scala.concurrent.ExecutionContext = system.dispatcher

  val applicationId: Option[String] =
    tokenPayload match {
      case t: CustomDamlJWTPayload => t.applicationId
      case t: StandardJWTPayload =>
        // For standard jwts, the JSON API uses the user id
        // as the application id on command submissions.
        Some(t.userId)
    }
}

final case class IdeLedgerClient(
    compiledPackages: PureCompiledPackages,
    traceLog: TraceLog,
    warningLog: WarningLog,
    canceled: () => Boolean,
) extends ScriptLedgerClient
