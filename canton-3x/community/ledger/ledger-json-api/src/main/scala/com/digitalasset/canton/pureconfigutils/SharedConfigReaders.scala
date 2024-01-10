// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pureconfigutils

import org.apache.pekko.http.scaladsl.model.Uri
import com.auth0.jwt.algorithms.Algorithm
import com.daml.jwt.{ECDSAVerifier, HMAC256Verifier, JwksVerifier, JwtVerifierBase, RSA256Verifier}
import com.digitalasset.canton.platform.services.time.TimeProviderType
import pureconfig.error.{CannotConvert, ConvertFailure, FailureReason}
import pureconfig.{ConfigObjectCursor, ConfigReader, ConvertHelpers}
import com.daml.jwt.{Error as JwtError}
import scalaz.\/
import scalaz.syntax.std.option.*

import java.nio.file.Path
import com.daml.metrics.api.reporters.MetricsReporter

final case class HttpServerConfig(
    address: String = com.digitalasset.canton.cliopts.Http.defaultAddress,
    port: Option[Int] = None,
    portFile: Option[Path] = None,
)

object TokenVerifierConfig {
  private val knownTokenVerifiers: Map[String, String => JwtError \/ JwtVerifierBase] =
    Map(
      "rs256-crt" -> (RSA256Verifier.fromCrtFile(_)),
      "es256-crt" -> (ECDSAVerifier
        .fromCrtFile(_, Algorithm.ECDSA256(_, null))),
      "es512-crt" -> (ECDSAVerifier
        .fromCrtFile(_, Algorithm.ECDSA512(_, null))),
      "rs256-jwks" -> (valueStr =>
        \/.attempt(JwksVerifier(valueStr))(e => JwtError(Symbol("RS256"), e.getMessage))
      ),
    )
  private val unsafeTokenVerifier: (String, String => JwtError \/ JwtVerifierBase) =
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
}
