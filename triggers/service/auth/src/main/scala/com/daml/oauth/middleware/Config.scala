// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.http.scaladsl.model.Uri
import com.auth0.jwt.algorithms.Algorithm
import com.daml.jwt.{ECDSAVerifier, HMAC256Verifier, JwksVerifier, JwtVerifierBase, RSA256Verifier}
import com.daml.ports.Port

case class Config(
    // Port the middleware listens on
    port: Port,
    // OAuth2 server endpoints
    oauthAuth: Uri,
    oauthToken: Uri,
    // OAuth2 client properties
    clientId: String,
    clientSecret: String,
    // Token verification
    tokenVerifier: JwtVerifierBase,
)

object Config {
  private val Empty =
    Config(
      port = Port.Dynamic,
      oauthAuth = null,
      oauthToken = null,
      clientId = null,
      clientSecret = null,
      tokenVerifier = null)

  def parseConfig(args: Seq[String]): Option[Config] =
    configParser.parse(args, Empty)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  val configParser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("oauth-middleware") {
      head("OAuth2 Middleware")

      opt[Int]("port")
        .action((x, c) => c.copy(port = Port(x)))
        .required()
        .text("Port to listen on")

      opt[String]("oauth-auth")
        .action((x, c) => c.copy(oauthAuth = Uri(x)))
        .required()
        .text("URI of the OAuth2 authorization endpoint")

      opt[String]("oauth-token")
        .action((x, c) => c.copy(oauthToken = Uri(x)))
        .required()
        .text("URI of the OAuth2 token endpoint")

      opt[String]("id").hidden
        .action((x, c) => c.copy(clientId = x))
        .withFallback(() => sys.env.getOrElse("DAML_CLIENT_ID", ""))
        .validate(x =>
          if (x.isEmpty) failure("Environment variable DAML_CLIENT_ID must not be empty")
          else success)

      opt[String]("secret").hidden
        .action((x, c) => c.copy(clientSecret = x))
        .withFallback(() => sys.env.getOrElse("DAML_CLIENT_SECRET", ""))
        .validate(x =>
          if (x.isEmpty) failure("Environment variable DAML_CLIENT_SECRET must not be empty")
          else success)

      opt[String]("auth-jwt-hs256-unsafe")
        .optional()
        .hidden()
        .validate(v => Either.cond(v.length > 0, (), "HMAC secret must be a non-empty string"))
        .text("[UNSAFE] Enables JWT-based authorization with shared secret HMAC256 signing: USE THIS EXCLUSIVELY FOR TESTING")
        .action((secret, config) =>
          config.copy(tokenVerifier = HMAC256Verifier(secret).valueOr(err =>
            sys.error(s"Failed to create HMAC256 verifier: $err"))))

      opt[String]("auth-jwt-rs256-crt")
        .optional()
        .validate(v =>
          Either.cond(v.length > 0, (), "Certificate file path must be a non-empty string"))
        .text("Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given X509 certificate file (.crt)")
        .action(
          (path, config) =>
            config.copy(
              tokenVerifier = RSA256Verifier
                .fromCrtFile(path)
                .valueOr(err => sys.error(s"Failed to create RSA256 verifier: $err"))))

      opt[String]("auth-jwt-es256-crt")
        .optional()
        .validate(v =>
          Either.cond(v.length > 0, (), "Certificate file path must be a non-empty string"))
        .text("Enables JWT-based authorization, where the JWT is signed by ECDSA256 with a public key loaded from the given X509 certificate file (.crt)")
        .action(
          (path, config) =>
            config.copy(
              tokenVerifier = ECDSAVerifier
                .fromCrtFile(path, Algorithm.ECDSA256(_, null))
                .valueOr(err => sys.error(s"Failed to create ECDSA256 verifier: $err"))))

      opt[String]("auth-jwt-es512-crt")
        .optional()
        .validate(v =>
          Either.cond(v.length > 0, (), "Certificate file path must be a non-empty string"))
        .text("Enables JWT-based authorization, where the JWT is signed by ECDSA512 with a public key loaded from the given X509 certificate file (.crt)")
        .action(
          (path, config) =>
            config.copy(
              tokenVerifier = ECDSAVerifier
                .fromCrtFile(path, Algorithm.ECDSA512(_, null))
                .valueOr(err => sys.error(s"Failed to create ECDSA512 verifier: $err"))))

      opt[String]("auth-jwt-rs256-jwks")
        .optional()
        .validate(v => Either.cond(v.length > 0, (), "JWK server URL must be a non-empty string"))
        .text("Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given JWKS URL")
        .action((url, config) => config.copy(tokenVerifier = JwksVerifier(url)))

      checkConfig { cfg =>
        if (cfg.tokenVerifier == null)
          Left("You must specify one of the --auth-jwt-* flags for token verification.")
        else
          Right(())
      }

      help("help").text("Print this usage text")

      note("""
        |Environment variables:
        |  DAML_CLIENT_ID      The OAuth2 client-id - must not be empty
        |  DAML_CLIENT_SECRET  The OAuth2 client-secret - must not be empty
        """.stripMargin)
    }
}
