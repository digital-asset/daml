// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import akka.http.scaladsl.model.Uri
import com.auth0.jwt.algorithms.Algorithm
import com.daml.jwt.{ECDSAVerifier, JwksVerifier, JwtVerifierBase, RSA256Verifier}

import pureconfig.{ConfigReader, ConfigSource, ConvertHelpers}
import pureconfig.error.ConfigReaderException
import scopt.OptionParser

import java.io.File
import pureconfig.generic.semiauto._

sealed trait ConfigError extends Product with Serializable {
  def msg: String
}
case object MissingConfigError extends ConfigError {
  val msg = "Missing auth middleware config file"
}
final case class ConfigParseError(msg: String) extends ConfigError

case class Cli(configFile: Option[File] = None) {
  import Cli._
  def loadConfig: Either[ConfigError, Config] = {
    configFile
      .map(f =>
        try {
          Right(ConfigSource.file(f).loadOrThrow[Config])
        } catch {
          case ex: ConfigReaderException[_] => Left(ConfigParseError(ex.failures.head.description))
        }
      )
      .getOrElse(Left(MissingConfigError))
  }
}

object Cli {
  implicit val tokenVerifierReader =
    ConfigReader.forProduct2[JwtVerifierBase, String, String]("type", "uri") { case (t, p) =>
      // rs256-crt, es256-crt, es512-crt, rs256-jwks
      t match {
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
  lazy implicit val uriReader =
    ConfigReader.fromString[Uri](ConvertHelpers.catchReadError(s => Uri(s)))
  lazy implicit val clientSecretReader =
    ConfigReader.fromString[SecretString](ConvertHelpers.catchReadError(s => SecretString(s)))
  lazy implicit val cfgReader: ConfigReader[Config] = deriveReader[Config]

  private val parser: OptionParser[Cli] = new scopt.OptionParser[Cli]("oauth-middleware") {
    help('h', "help").text("Print usage")
    opt[Option[File]]('c', "config")
      .text("Set app configuration file")
      .valueName("<file1>")
      .action((file, cli) => cli.copy(configFile = file))

    checkConfig(cli =>
      if (cli.configFile.isEmpty) {
        failure("config file needs to be supplied using `-c` flag")
      } else success
    )

    override def showUsageOnError: Option[Boolean] = Some(true)
  }

  def parse(args: Array[String]): Option[Cli] = parser.parse(args, Cli())
}
