// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.time.{Clock, Instant, ZoneId}

import com.daml.grpc.test.GrpcServer
import com.daml.nonrepudiation.SignedPayloadRepository.KeyEncoder
import com.daml.nonrepudiation.client.TestSigningInterceptors
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{Channel, StatusRuntimeException}
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

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
            TestSigningInterceptors.signEverything(privateKey, certificate),
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

    val (privateKey, certificate) = testing.generateKeyAndCertificate()

    NonRepudiationProxy
      .owner(
        channel,
        proxyBuilder,
        certificates,
        signatures,
        Clock.systemUTC(),
        Health.Name,
      )
      .use { _ =>
        the[StatusRuntimeException] thrownBy {
          Health.getHealthStatus(
            proxyChannel,
            TestSigningInterceptors.signEverything(privateKey, certificate),
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

    def newInstance[Key: KeyEncoder]: Setup[Key] = {
      val certificates = new testing.Certificates
      val signatures = new testing.SignedPayloads
      val proxyName = InProcessServerBuilder.generateName()
      val proxyBuilder = InProcessServerBuilder.forName(proxyName)
      val proxyChannel = InProcessChannelBuilder.forName(proxyName).build()
      val (privateKey, certificate) = testing.generateKeyAndCertificate()
      certificates.put(certificate)
      Setup(certificates, signatures, privateKey, certificate, proxyBuilder, proxyChannel)
    }

  }

}
