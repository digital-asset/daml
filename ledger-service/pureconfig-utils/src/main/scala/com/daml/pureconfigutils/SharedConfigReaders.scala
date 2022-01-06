// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.pureconfigutils

import akka.http.scaladsl.model.Uri
import com.auth0.jwt.algorithms.Algorithm
import com.daml.dbutils.JdbcConfig
import com.daml.jwt.{ECDSAVerifier, HMAC256Verifier, JwksVerifier, JwtVerifierBase, RSA256Verifier}
import com.daml.platform.services.time.TimeProviderType
import pureconfig.{ConfigReader, ConvertHelpers}
import pureconfig.generic.semiauto.deriveReader

import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration

final case class HttpServerConfig(address: String, port: Int, portFile: Option[Path] = None)
final case class LedgerApiConfig(address: String, port: Int)
final case class MetricsConfig(reporter: String, reportingInterval: FiniteDuration)

object SharedConfigReaders {

  implicit val tokenVerifierReader: ConfigReader[JwtVerifierBase] =
    ConfigReader.forProduct2[JwtVerifierBase, String, String]("type", "uri") {
      case (t: String, p: String) =>
        // hs256-unsafe, rs256-crt, es256-crt, es512-crt, rs256-jwks
        t match {
          case "hs256-unsafe" =>
            HMAC256Verifier(p)
              .valueOr(err => sys.error(s"Failed to create HMAC256 verifier: $err"))
          case "rs256-crt" =>
            RSA256Verifier
              .fromCrtFile(p)
              .valueOr(err => sys.error(s"Failed to create RSA256 verifier: $err"))
          case "es256-crt" =>
            ECDSAVerifier
              .fromCrtFile(p, Algorithm.ECDSA256(_, null))
              .valueOr(err => sys.error(s"Failed to create ECDSA256 verifier: $err"))
          case "es512-crt" =>
            ECDSAVerifier
              .fromCrtFile(p, Algorithm.ECDSA512(_, null))
              .valueOr(err => sys.error(s"Failed to create ECDSA512 verifier: $err"))
          case "rs256-jwks" =>
            JwksVerifier(p)
        }
    }

  implicit val uriCfgReader: ConfigReader[Uri] =
    ConfigReader.fromString[Uri](ConvertHelpers.catchReadError(s => Uri(s)))

  implicit val timeProviderTypeCfgReader: ConfigReader[TimeProviderType] =
    ConfigReader.fromString[TimeProviderType](ConvertHelpers.catchReadError { s =>
      s.toLowerCase() match {
        case "static" => TimeProviderType.Static
        case "wall-clock" => TimeProviderType.WallClock
        case s =>
          throw new IllegalArgumentException(
            s"Value '$s' for time-provider-type is not one of 'static' or 'wall-clock'"
          )
      }
    })
  implicit val jdbcCfgReader: ConfigReader[JdbcConfig] = deriveReader[JdbcConfig]

  implicit val httpServerCfgReader: ConfigReader[HttpServerConfig] =
    deriveReader[HttpServerConfig]
  implicit val ledgerApiConfReader: ConfigReader[LedgerApiConfig] =
    deriveReader[LedgerApiConfig]
  implicit val metricsConfigReader: ConfigReader[MetricsConfig] = deriveReader[MetricsConfig]
}
