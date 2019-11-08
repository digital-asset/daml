// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jwt

import java.net.{URI, URL}
import java.security.interfaces.RSAPublicKey
import java.util.concurrent.TimeUnit

import com.auth0.jwk.UrlJwkProvider
import com.digitalasset.jwt.JwtVerifier.Error
import com.google.common.cache.{Cache, CacheBuilder}
import scalaz.{-\/, Show, \/}
import scalaz.syntax.show._

/**
  * A JWK verifier, where the public keys are automatically fetched from the given JWKS URL.
  *
  */
class JwksVerifier(url: URL) extends JwtVerifierBase {
  private[this] val http = new UrlJwkProvider(url)

  private[this] val cache: Cache[String, JwtVerifier] = CacheBuilder
    .newBuilder()
    .maximumSize(10)
    .expireAfterWrite(10, TimeUnit.HOURS)
    .build()

  private[this] def getVerifier(keyId: String): Error \/ JwtVerifier = {
    val jwk = http.get(keyId)
    val publicKey = jwk.getPublicKey.asInstanceOf[RSAPublicKey]
    RSA256Verifier(publicKey)
  }

  /** Looks up the verifier for the given keyId from the local cache.
    * On a cache miss, creates a new verifier by fetching the public key from the JWKS URL. */
  private[this] def getCachedVerifier(keyId: String): Error \/ JwtVerifier = {
    if (keyId == null)
      -\/(Error('getCachedVerifier, "No Key ID found"))
    else
      \/.fromTryCatchNonFatal(
        cache.get(keyId, () => getVerifier(keyId).fold(e => sys.error(e.shows), x => x))
      ).leftMap(e => Error('getCachedVerifier, e.getMessage))
  }

  def verify(jwt: domain.Jwt): Error \/ domain.DecodedJwt[String] = {
    for {
      keyId <- \/.fromTryCatchNonFatal(com.auth0.jwt.JWT.decode(jwt.value).getKeyId)
        .leftMap(e => Error('verify, e.getMessage))
      verifier <- getCachedVerifier(keyId)
      decoded <- verifier.verify(jwt)
    } yield decoded
  }
}

object JwksVerifier {
  def apply(url: String) = new JwksVerifier(new URI(url).toURL)

  final case class Error(what: Symbol, message: String)

  object Error {
    implicit val showInstance: Show[Error] =
      Show.shows(e => s"JwksVerifier.Error: ${e.what}, ${e.message}")
  }
}
