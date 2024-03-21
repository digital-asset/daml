// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import java.nio.file.Paths

import com.auth0.jwt.algorithms.Algorithm

import scala.util.Try

object JwtVerifierConfigurationCli {
  def parse[C](parser: scopt.OptionParser[C])(setter: (JwtVerifierBase, C) => C): Unit = {
    def setJwtVerifier(jwtVerifier: JwtVerifierBase, c: C): C = setter(jwtVerifier, c)

    import parser.opt

    opt[String]("auth-jwt-hs256-unsafe")
      .optional()
      .hidden()
      .validate(v => Either.cond(v.nonEmpty, (), "HMAC secret must be a non-empty string"))
      .text(
        "[UNSAFE] Enables JWT-based authorization with shared secret HMAC256 signing: USE THIS EXCLUSIVELY FOR TESTING"
      )
      .action { (secret, config) =>
        val verifier = HMAC256Verifier(secret)
          .valueOr(err => sys.error(s"Failed to create HMAC256 verifier: $err"))
        setJwtVerifier(verifier, config)
      }

    opt[String]("auth-jwt-rs256-crt")
      .optional()
      .validate(
        validatePath(_, "The certificate file specified via --auth-jwt-rs256-crt does not exist")
      )
      .text(
        "Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given X509 certificate file (.crt)"
      )
      .action { (path, config) =>
        val verifier = RSA256Verifier
          .fromCrtFile(path)
          .valueOr(err => sys.error(s"Failed to create RSA256 verifier: $err"))
        setJwtVerifier(verifier, config)
      }

    opt[String]("auth-jwt-es256-crt")
      .optional()
      .validate(
        validatePath(_, "The certificate file specified via --auth-jwt-es256-crt does not exist")
      )
      .text(
        "Enables JWT-based authorization, where the JWT is signed by ECDSA256 with a public key loaded from the given X509 certificate file (.crt)"
      )
      .action { (path, config) =>
        val verifier = ECDSAVerifier
          .fromCrtFile(path, Algorithm.ECDSA256(_, null))
          .valueOr(err => sys.error(s"Failed to create ECDSA256 verifier: $err"))
        setJwtVerifier(verifier, config)
      }

    opt[String]("auth-jwt-es512-crt")
      .optional()
      .validate(
        validatePath(_, "The certificate file specified via --auth-jwt-es512-crt does not exist")
      )
      .text(
        "Enables JWT-based authorization, where the JWT is signed by ECDSA512 with a public key loaded from the given X509 certificate file (.crt)"
      )
      .action { (path, config) =>
        val verifier = ECDSAVerifier
          .fromCrtFile(path, Algorithm.ECDSA512(_, null))
          .valueOr(err => sys.error(s"Failed to create ECDSA512 verifier: $err"))
        setJwtVerifier(verifier, config)
      }

    opt[String]("auth-jwt-rs256-jwks")
      .optional()
      .validate(v => Either.cond(v.length > 0, (), "JWK server URL must be a non-empty string"))
      .text(
        "Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given JWKS URL"
      )
      .action { (url, config) =>
        val verifier = JwksVerifier(url)
        setJwtVerifier(verifier, config)
      }

    ()
  }

  private def validatePath(path: String, message: String): Either[String, Unit] = {
    val valid = Try(Paths.get(path).toFile.canRead).getOrElse(false)
    if (valid) Right(()) else Left(message)
  }
}
