// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import java.math.BigInteger
import java.nio.file.{Files, Path}
import java.security.interfaces.{ECPrivateKey, ECPublicKey, RSAPrivateKey, RSAPublicKey}
import java.security.{KeyPair, KeyPairGenerator, PrivateKey, PublicKey, Security}
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.daml.fs.TemporaryDirectory
import com.daml.jwt.JwtVerifierConfigurationCliSpec._
import com.daml.ledger.api.auth.ClaimSet.Claims
import com.daml.ledger.api.auth.{AuthService, AuthServiceJWT, AuthServiceWildcard, ClaimPublic}
import com.daml.testing.SimpleHttpServer
import io.grpc.Metadata
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.cert.jcajce.{JcaX509CertificateConverter, JcaX509v3CertificateBuilder}
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scopt.OptionParser

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._

class JwtVerifierConfigurationCliSpec extends AsyncWordSpec with Matchers {
  Security.addProvider(new BouncyCastleProvider)

  "auth command-line parsers" should {
    "parse and configure the authorisation mechanism correctly when `--auth-jwt-hs256-unsafe <secret>` is passed" in {
      val secret = "someSecret"
      val authService = parseConfig(Array("--auth-jwt-hs256-unsafe", secret))
      val token = JWT.create().sign(Algorithm.HMAC256(secret))
      val metadata = createAuthMetadata(token)
      decodeAndCheckMetadata(authService, metadata)
    }

    "parse and configure the authorisation mechanism correctly when `--auth-jwt-rs256-crt <PK.crt>` is passed" in
      new TemporaryDirectory(getClass.getSimpleName).use { directory =>
        val (publicKey, privateKey) = newRsaKeyPair()
        val certificatePath = newCertificate("SHA256WithRSA", directory, publicKey, privateKey)
        val token = JWT.create().sign(Algorithm.RSA256(publicKey, privateKey))

        val authService = parseConfig(Array("--auth-jwt-rs256-crt", certificatePath.toString))
        val metadata = createAuthMetadata(token)
        decodeAndCheckMetadata(authService, metadata)
      }

    "parse and configure the authorisation mechanism correctly when `--auth-jwt-es256-crt <PK.crt>` is passed" in
      new TemporaryDirectory(getClass.getSimpleName).use { directory =>
        val (publicKey, privateKey) = newEcdsaKeyPair()
        val certificatePath = newCertificate("SHA256WithECDSA", directory, publicKey, privateKey)
        val token = JWT.create().sign(Algorithm.ECDSA256(publicKey, privateKey))

        val authService = parseConfig(Array("--auth-jwt-es256-crt", certificatePath.toString))
        val metadata = createAuthMetadata(token)
        decodeAndCheckMetadata(authService, metadata)
      }

    "parse and configure the authorisation mechanism correctly when `--auth-jwt-es512-crt <PK.crt>` is passed" in
      new TemporaryDirectory(getClass.getSimpleName).use { directory =>
        val (publicKey, privateKey) = newEcdsaKeyPair()
        val certificatePath = newCertificate("SHA512WithECDSA", directory, publicKey, privateKey)
        val token = JWT.create().sign(Algorithm.ECDSA512(publicKey, privateKey))

        val authService = parseConfig(Array("--auth-jwt-es512-crt", certificatePath.toString))
        val metadata = createAuthMetadata(token)
        decodeAndCheckMetadata(authService, metadata)
      }

    "parse and configure the authorisation mechanism correctly when `--auth-jwt-rs256-jwks <URL>` is passed" in {
      val (publicKey, privateKey) = newRsaKeyPair()
      val keyId = "test-key-1"
      val token = JWT.create().withKeyId(keyId).sign(Algorithm.RSA256(publicKey, privateKey))

      // Start a JWKS server and create a verifier using the JWKS server
      val jwks = KeyUtils.generateJwks(
        Map(
          keyId -> publicKey
        )
      )

      val server = SimpleHttpServer.start(jwks)
      Future {
        val url = SimpleHttpServer.responseUrl(server)
        val authService = parseConfig(Array("--auth-jwt-rs256-jwks", url))
        val metadata = createAuthMetadata(token)
        (authService, metadata)
      }.flatMap { case (authService, metadata) =>
        decodeAndCheckMetadata(authService, metadata)
      }.andThen { case _ =>
        SimpleHttpServer.stop(server)
      }
    }
  }
}

object JwtVerifierConfigurationCliSpec {
  private def parseConfig(args: Array[String]): AuthService = {
    val parser = new OptionParser[AtomicReference[AuthService]]("test") {}
    JwtVerifierConfigurationCli.parse(parser) { (verifier, config) =>
      config.set(AuthServiceJWT(verifier))
      config
    }
    parser.parse(args, new AtomicReference[AuthService](AuthServiceWildcard)).get.get()
  }

  private def createAuthMetadata(token: String) = {
    val metadata = new Metadata()
    metadata.put(AuthService.AUTHORIZATION_KEY, s"Bearer $token")
    metadata
  }

  private def decodeAndCheckMetadata(
      authService: AuthService,
      metadata: Metadata,
  )(implicit executionContext: ExecutionContext): Future[Assertion] = {
    import org.scalatest.Inside._
    import org.scalatest.matchers.should.Matchers._

    authService.decodeMetadata(metadata).asScala.map { auth =>
      inside(auth) { case claims: Claims =>
        claims.claims should be(List(ClaimPublic))
      }
    }
  }

  private def newRsaKeyPair(): (RSAPublicKey, RSAPrivateKey) = {
    val keyPair = newKeyPair("RSA", 2048)
    val publicKey = keyPair.getPublic.asInstanceOf[RSAPublicKey]
    val privateKey = keyPair.getPrivate.asInstanceOf[RSAPrivateKey]
    (publicKey, privateKey)
  }

  private def newEcdsaKeyPair(): (ECPublicKey, ECPrivateKey) = {
    val keyPair = newKeyPair("ECDSA", 256)
    val publicKey = keyPair.getPublic.asInstanceOf[ECPublicKey]
    val privateKey = keyPair.getPrivate.asInstanceOf[ECPrivateKey]
    (publicKey, privateKey)
  }

  private def newKeyPair(algorithm: String, keySize: Int): KeyPair = {
    val generator = KeyPairGenerator.getInstance(algorithm, BouncyCastleProvider.PROVIDER_NAME)
    generator.initialize(keySize)
    generator.generateKeyPair()
  }

  private def newCertificate(
      signatureAlgorithm: String,
      directory: Path,
      publicKey: PublicKey,
      privateKey: PrivateKey,
  ): Path = {
    val now = Instant.now()
    val dnName = new X500Name(s"CN=${getClass.getSimpleName}")
    val contentSigner = new JcaContentSignerBuilder(signatureAlgorithm).build(privateKey)
    val certBuilder = new JcaX509v3CertificateBuilder(
      dnName,
      BigInteger.valueOf(now.toEpochMilli),
      java.util.Date.from(now),
      java.util.Date.from(now.plusSeconds(60)),
      dnName,
      publicKey,
    )
    val certificate =
      new JcaX509CertificateConverter()
        .setProvider(BouncyCastleProvider.PROVIDER_NAME)
        .getCertificate(certBuilder.build(contentSigner))
    val certificatePath = directory.resolve("certificate")
    Files.write(certificatePath, certificate.getEncoded)
    certificatePath
  }
}
