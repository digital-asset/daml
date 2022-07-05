// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import scopt.{OParser, OParserBuilder}

import java.nio.file.Paths
import scala.util.Try

/*
This class is copying `JwtVerifierConfigurationCli`, but sets `AuthServiceConfig` instead of `JwtVerifierBase`.
This will have to be cleaned up, as soon as Cli for SoX will be switched completely over HOCON configuration.
 */
object AuthServiceConfigCli {
  def parse[C, Extra](
      builder: OParserBuilder[C]
  )(setter: (AuthServiceConfig, C) => C): OParser[String, C] = {
    def setAuthServiceConfig(authServiceConfig: AuthServiceConfig, c: C): C =
      setter(authServiceConfig, c)

    import builder._

    OParser.sequence(
      opt[String]("auth-jwt-hs256-unsafe")
        .optional()
        .hidden()
        .validate(v => Either.cond(v.nonEmpty, (), "HMAC secret must be a non-empty string"))
        .text(
          "[UNSAFE] Enables JWT-based authorization with shared secret HMAC256 signing: USE THIS EXCLUSIVELY FOR TESTING"
        )
        .action { (secret, config) =>
          setAuthServiceConfig(AuthServiceConfig.UnsafeJwtHmac256(secret), config)
        },
      opt[String]("auth-jwt-rs256-crt")
        .optional()
        .validate(
          validatePath(_, "The certificate file specified via --auth-jwt-rs256-crt does not exist")
        )
        .text(
          "Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given X509 certificate file (.crt)"
        )
        .action { (path, config) =>
          setAuthServiceConfig(AuthServiceConfig.JwtRs256(path), config)
        },
      opt[String]("auth-jwt-es256-crt")
        .optional()
        .validate(
          validatePath(_, "The certificate file specified via --auth-jwt-es256-crt does not exist")
        )
        .text(
          "Enables JWT-based authorization, where the JWT is signed by ECDSA256 with a public key loaded from the given X509 certificate file (.crt)"
        )
        .action { (path, config) =>
          setAuthServiceConfig(AuthServiceConfig.JwtEs256(path), config)
        },
      opt[String]("auth-jwt-es512-crt")
        .optional()
        .validate(
          validatePath(_, "The certificate file specified via --auth-jwt-es512-crt does not exist")
        )
        .text(
          "Enables JWT-based authorization, where the JWT is signed by ECDSA512 with a public key loaded from the given X509 certificate file (.crt)"
        )
        .action { (path, config) =>
          setAuthServiceConfig(AuthServiceConfig.JwtEs512(path), config)
        },
      opt[String]("auth-jwt-rs256-jwks")
        .optional()
        .validate(v => Either.cond(v.length > 0, (), "JWK server URL must be a non-empty string"))
        .text(
          "Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given JWKS URL"
        )
        .action { (url, config) =>
          setAuthServiceConfig(AuthServiceConfig.JwtRs256Jwks(url), config)
        },
    )
  }

  private def validatePath(path: String, message: String): Either[String, Unit] = {
    val valid = Try(Paths.get(path).toFile.canRead).getOrElse(false)
    if (valid) Right(()) else Left(message)
  }
}
