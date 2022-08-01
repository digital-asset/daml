// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.pureconfigutils

import akka.http.scaladsl.model.Uri
import com.auth0.jwt.algorithms.Algorithm
import com.daml.dbutils.JdbcConfig
import com.daml.jwt.{
  ECDSAVerifier,
  HMAC256Verifier,
  JwksVerifier,
  JwtVerifier,
  JwtVerifierBase,
  RSA256Verifier,
}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.metrics.MetricsReporter
import com.daml.platform.services.time.TimeProviderType
import pureconfig.error.{CannotConvert, ConvertFailure, FailureReason}
import pureconfig.{ConfigObjectCursor, ConfigReader, ConvertHelpers}
import pureconfig.generic.semiauto.deriveReader
import scalaz.\/
import scalaz.syntax.std.option._

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

object TokenVerifierConfig {
  private val knownTokenVerifiers: Map[String, String => JwtVerifier.Error \/ JwtVerifierBase] =
    Map(
      "rs256-crt" -> (RSA256Verifier.fromCrtFile(_)),
      "es256-crt" -> (ECDSAVerifier
        .fromCrtFile(_, Algorithm.ECDSA256(_, null))),
      "es512-crt" -> (ECDSAVerifier
        .fromCrtFile(_, Algorithm.ECDSA512(_, null))),
      "rs256-jwks" -> (valueStr =>
        \/.attempt(JwksVerifier(valueStr))(e => JwtVerifier.Error(Symbol("RS256"), e.getMessage))
      ),
    )
  private val unsafeTokenVerifier: (String, String => JwtVerifier.Error \/ JwtVerifierBase) =
    "hs256-unsafe" -> (HMAC256Verifier(_))

  def extractByType(
      typeStr: String,
      valueStr: String,
      objectCursor: ConfigObjectCursor,
  ): ConfigReader.Result[JwtVerifierBase] = {
    def convertFailure(msg: String) = {
      ConfigReader.Result.fail(
        ConvertFailure(
          CannotConvert(typeStr, "JwtVerifier", msg),
          objectCursor,
        )
      )
    }
    (knownTokenVerifiers + unsafeTokenVerifier)
      .get(typeStr)
      .cata(
        { conv =>
          conv(valueStr).fold(
            err => convertFailure(s"Failed to create $typeStr verifier: $err"),
            (Right(_)),
          )
        },
        convertFailure(s"value not one of ${knownTokenVerifiers.keys.mkString(", ")}"),
      )
  }
}
object SharedConfigReaders {

  def catchConvertError[A, B](f: String => Either[String, B])(implicit
      B: reflect.ClassTag[B]
  ): String => Either[FailureReason, B] =
    s => f(s).left.map(CannotConvert(s, B.toString, _))

  implicit val tokenVerifierCfgRead: ConfigReader[JwtVerifierBase] =
    ConfigReader.fromCursor { cur =>
      for {
        objCur <- cur.asObjectCursor
        typeCur <- objCur.atKey("type")
        typeStr <- typeCur.asString
        valueCur <- objCur.atKey("uri")
        valueStr <- valueCur.asString
        ident <- TokenVerifierConfig.extractByType(typeStr, valueStr, objCur)
      } yield ident
    }

  implicit val uriCfgReader: ConfigReader[Uri] =
    ConfigReader.fromString[Uri](ConvertHelpers.catchReadError(s => Uri(s)))

  implicit val timeProviderTypeCfgReader: ConfigReader[TimeProviderType] = {
    ConfigReader.fromString[TimeProviderType](catchConvertError { s =>
      s.toLowerCase() match {
        case "static" => Right(TimeProviderType.Static)
        case "wall-clock" => Right(TimeProviderType.WallClock)
        case _ => Left("not one of 'static' or 'wall-clock'")
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
