// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.pureconfigutils

import akka.http.scaladsl.model.Uri
import com.auth0.jwt.algorithms.Algorithm
import com.daml.dbutils.JdbcConfig
import com.daml.jwt.{ECDSAVerifier, HMAC256Verifier, JwksVerifier, JwtVerifierBase, RSA256Verifier}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.metrics.MetricsReporter
import com.daml.platform.services.time.TimeProviderType
import pureconfig.error.{CannotConvert, FailureReason}
import pureconfig.{ConfigReader, ConvertHelpers}
import pureconfig.generic.semiauto.deriveReader

import java.nio.file.Path
import java.io.File
import scala.concurrent.duration.FiniteDuration

final case class HttpServerConfig(address: String, port: Int, portFile: Option[Path] = None)
final case class LedgerTlsConfig(
    enabled: Boolean = false,
    certChainFile: Option[File] = None,
    privateKeyFile: Option[File] = None,
    trustCollectionFile: Option[File] = None,
) {
  def tlsConfiguration: TlsConfiguration =
    TlsConfiguration(enabled, certChainFile, privateKeyFile, trustCollectionFile)
}
final case class LedgerApiConfig(
    address: String,
    port: Int,
    tls: LedgerTlsConfig = LedgerTlsConfig(),
)
final case class MetricsConfig(reporter: MetricsReporter, reportingInterval: FiniteDuration)

object SharedConfigReaders {

  def catchConvertError[A, B](f: String => Either[String, B])(implicit
      B: reflect.ClassTag[B]
  ): String => Either[FailureReason, B] =
    s => f(s).left.map(CannotConvert(s, B.toString, _))

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

  implicit val timeProviderTypeCfgReader: ConfigReader[TimeProviderType] = {
    ConfigReader.fromString[TimeProviderType](catchConvertError { s =>
      s.toLowerCase() match {
        case "static" => Right(TimeProviderType.Static)
        case "wall-clock" => Right(TimeProviderType.WallClock)
        case _ => Left("value  is not one of 'static' or 'wall-clock'")
      }
    })
  }

  implicit val metricReporterReader: ConfigReader[MetricsReporter] = {
    ConfigReader.fromString[MetricsReporter](ConvertHelpers.catchReadError { s =>
      MetricsReporter.parseMetricsReporter(s.toLowerCase())
    })
  }
  implicit val jdbcCfgReader: ConfigReader[JdbcConfig] = deriveReader[JdbcConfig]

  implicit val httpServerCfgReader: ConfigReader[HttpServerConfig] =
    deriveReader[HttpServerConfig]

  implicit val ledgerTlsCfgReader: ConfigReader[LedgerTlsConfig] =
    deriveReader[LedgerTlsConfig]
  implicit val ledgerApiConfReader: ConfigReader[LedgerApiConfig] =
    deriveReader[LedgerApiConfig]
  implicit val metricsConfigReader: ConfigReader[MetricsConfig] = deriveReader[MetricsConfig]
}
