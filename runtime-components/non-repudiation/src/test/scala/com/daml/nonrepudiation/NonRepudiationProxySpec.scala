// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.{KeyPairGenerator, PublicKey}

import com.daml.grpc.test.GrpcServer
import com.daml.nonrepudiation.SignedPayloadRepository.KeyEncoder
import com.daml.nonrepudiation.client.SigningInterceptor
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{Channel, StatusRuntimeException}
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.concurrent.TrieMap

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
    val Setup(keys, signedPayloads, proxyBuilder, proxyChannel) = Setup.newInstance[String]
    val keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair()
    keys.put(keyPair.getPublic)

    NonRepudiationProxy
      .owner(
        participant = channel,
        serverBuilder = proxyBuilder,
        keyRepository = keys,
        signedPayloadRepository = signedPayloads,
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
          FingerprintBytes.compute(keyPair.getPublic)

        val expectedSignature =
          SignatureBytes.sign(
            expectedAlgorithm,
            keyPair.getPrivate,
            expectedPayload,
          )

        val expectedSignedPayload =
          SignedPayload(
            expectedAlgorithm,
            expectedFingerprint,
            expectedPayload,
            expectedSignature,
          )

        val result =
          Health.getHealthStatus(
            proxyChannel,
            new SigningInterceptor(keyPair, expectedAlgorithm),
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
    val Setup(keys, signatures, proxyBuilder, proxyChannel) = Setup.newInstance[String]

    NonRepudiationProxy
      .owner(
        participant = channel,
        serverBuilder = proxyBuilder,
        keyRepository = keys,
        signedPayloadRepository = signatures,
        Health.Name,
      )
      .use { _ =>
        the[StatusRuntimeException] thrownBy {
          Health.getHealthStatus(proxyChannel)
        } should have message SignatureVerificationFailed.asRuntimeException.getMessage
      }
  }

  it should "reject requests with an unknown signature" in withServices(
    Health.newInstance,
    Reflection.newInstance,
  ) { channel =>
    val Setup(keys, signatures, proxyBuilder, proxyChannel) = Setup.newInstance[String]
    val keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair()

    NonRepudiationProxy
      .owner(channel, proxyBuilder, keys, signatures, Health.Name)
      .use { _ =>
        the[StatusRuntimeException] thrownBy {
          Health.getHealthStatus(
            proxyChannel,
            new SigningInterceptor(keyPair, AlgorithmString.SHA256withRSA),
          )
        } should have message SignatureVerificationFailed.asRuntimeException.getMessage
      }
  }

}

object NonRepudiationProxySpec {

  final case class Setup[Key](
      keys: KeyRepository,
      signedPayloads: SignedPayloadRepository[Key],
      proxyBuilder: InProcessServerBuilder,
      proxyChannel: Channel,
  )

  object Setup {

    def newInstance[Key: KeyEncoder]: Setup[Key] = {
      val keys = new Keys
      val signatures = new SignedPayloads
      val proxyName = InProcessServerBuilder.generateName()
      val proxyBuilder = InProcessServerBuilder.forName(proxyName)
      val proxyChannel = InProcessChannelBuilder.forName(proxyName).build()
      Setup(keys, signatures, proxyBuilder, proxyChannel)
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

  final class Keys(keys: PublicKey*) extends KeyRepository {

    private val map: TrieMap[FingerprintBytes, PublicKey] = TrieMap(
      keys.map(key => FingerprintBytes.compute(key) -> key): _*
    )

    override def get(fingerprint: FingerprintBytes): Option[PublicKey] =
      map.get(fingerprint)

    override def put(key: PublicKey): FingerprintBytes = {
      val fingerprint = FingerprintBytes.compute(key)
      map.put(fingerprint, key)
      fingerprint
    }
  }

}
