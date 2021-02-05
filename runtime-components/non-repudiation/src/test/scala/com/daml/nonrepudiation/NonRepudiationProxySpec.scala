// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.time.{Clock, Duration, Instant, ZoneId}

import com.daml.grpc.test.GrpcServer
import com.daml.nonrepudiation.SignedPayloadRepository.KeyEncoder
import com.daml.nonrepudiation.client.SigningInterceptor
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{Channel, StatusRuntimeException}
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import sun.security.tools.keytool.CertAndKeyGen
import sun.security.x509.X500Name

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

final class NonRepudiationProxySpec
    extends AsyncFlatSpec
    with Matchers
    with Inside
    with GrpcServer {

  import NonRepudiationProxySpec._
  import Services._
  import SignatureVerificationInterceptor.SignatureVerificationFailed

  behavior of "NonRepudiationProxy"

  it should "accept requests signed with a known key and add the correct signature" in withServices(
    Health.newInstance,
    Reflection.newInstance,
  ) { channel =>
    val Setup(certificates, signedPayloads, privateKey, certificate, proxyBuilder, proxyChannel) =
      Setup.newInstance[String]
    val expectedTimestamp = Instant.ofEpochMilli(42)
    val timestampProvider = Clock.fixed(expectedTimestamp, ZoneId.systemDefault())

    NonRepudiationProxy
      .owner(
        participant = channel,
        serverBuilder = proxyBuilder,
        certificateRepository = certificates,
        signedPayloadRepository = signedPayloads,
        metricsReporterProvider = MetricsReporterOwner.noop[ExecutionContext],
        timestampProvider = timestampProvider,
        Health.Name,
      )
      .use { _ =>
        val expectedAlgorithm =
          AlgorithmString.SHA256withRSA

        val expectedPayload =
          PayloadBytes.wrap(Health.Requests.Check.toByteArray)

        val expectedKey =
          signedPayloads.keyEncoder.encode(expectedPayload)

        val expectedFingerprint =
          FingerprintBytes.compute(certificate)

        val expectedSignature =
          SignatureBytes.sign(
            expectedAlgorithm,
            privateKey,
            expectedPayload,
          )

        val expectedSignedPayload =
          SignedPayload(
            expectedAlgorithm,
            expectedFingerprint,
            expectedPayload,
            expectedSignature,
            expectedTimestamp,
          )

        val result =
          Health.getHealthStatus(
            proxyChannel,
            new SigningInterceptor(privateKey, certificate),
          )

        result shouldEqual Health.getHealthStatus(channel)

        inside(signedPayloads.get(expectedKey)) { case signedPayloads =>
          signedPayloads should contain only expectedSignedPayload
        }

      }
  }

  it should "reject unsigned requests" in withServices(
    Health.newInstance,
    Reflection.newInstance,
  ) { channel =>
    val Setup(certificates, signatures, _, _, proxyBuilder, proxyChannel) =
      Setup.newInstance[String]

    NonRepudiationProxy
      .owner(
        participant = channel,
        serverBuilder = proxyBuilder,
        certificateRepository = certificates,
        signedPayloadRepository = signatures,
        metricsReporterProvider = MetricsReporterOwner.noop[ExecutionContext],
        timestampProvider = Clock.systemUTC(),
        Health.Name,
      )
      .use { _ =>
        the[StatusRuntimeException] thrownBy {
          Health.getHealthStatus(proxyChannel)
        } should have message SignatureVerificationFailed.asRuntimeException.getMessage
      }
  }

  it should "reject requests signed with an unknown key" in withServices(
    Health.newInstance,
    Reflection.newInstance,
  ) { channel =>
    val Setup(certificates, signatures, _, _, proxyBuilder, proxyChannel) =
      Setup.newInstance[String]

    val (privateKey, certificate) = Setup.generateKeyAndCertificate()

    NonRepudiationProxy
      .owner(
        channel,
        proxyBuilder,
        certificates,
        signatures,
        MetricsReporterOwner.noop[ExecutionContext],
        Clock.systemUTC(),
        Health.Name,
      )
      .use { _ =>
        the[StatusRuntimeException] thrownBy {
          Health.getHealthStatus(
            proxyChannel,
            new SigningInterceptor(privateKey, certificate),
          )
        } should have message SignatureVerificationFailed.asRuntimeException.getMessage
      }
  }

}

object NonRepudiationProxySpec {

  final case class Setup[Key](
      certificates: CertificateRepository,
      signedPayloads: SignedPayloadRepository[Key],
      privateKey: PrivateKey,
      certificate: X509Certificate,
      proxyBuilder: InProcessServerBuilder,
      proxyChannel: Channel,
  )

  object Setup {

    def generateKeyAndCertificate(): (PrivateKey, X509Certificate) = {
      val generator = new CertAndKeyGen(AlgorithmString.RSA, AlgorithmString.SHA256withRSA)
      generator.generate(2048)
      val privateKey = generator.getPrivateKey
      val certificate = generator.getSelfCertificate(
        new X500Name("CN=Non-Repudiation Test,O=Digital Asset,L=Zurich,C=CH"),
        Duration.ofHours(1).getSeconds,
      )
      (privateKey, certificate)
    }

    def newInstance[Key: KeyEncoder]: Setup[Key] = {
      val certificates = new Certificates
      val signatures = new SignedPayloads
      val proxyName = InProcessServerBuilder.generateName()
      val proxyBuilder = InProcessServerBuilder.forName(proxyName)
      val proxyChannel = InProcessChannelBuilder.forName(proxyName).build()
      val (privateKey, certificate) = generateKeyAndCertificate()
      certificates.put(certificate)
      Setup(certificates, signatures, privateKey, certificate, proxyBuilder, proxyChannel)
    }

  }

  final class SignedPayloads[Key: KeyEncoder] extends SignedPayloadRepository[Key] {
    private val map = TrieMap.empty[Key, SignedPayload]
    override def put(signedPayload: SignedPayload): Unit = {
      val _ = map.put(keyEncoder.encode(signedPayload.payload), signedPayload)
    }

    override def get(key: Key): Iterable[SignedPayload] =
      map.get(key).toList
  }

  final class Certificates extends CertificateRepository {

    private val map = TrieMap.empty[FingerprintBytes, X509Certificate]

    override def get(fingerprint: FingerprintBytes): Option[X509Certificate] =
      map.get(fingerprint)

    override def put(certificate: X509Certificate): FingerprintBytes = {
      val fingerprint = FingerprintBytes.compute(certificate)
      map.put(fingerprint, certificate)
      fingerprint
    }
  }

}
