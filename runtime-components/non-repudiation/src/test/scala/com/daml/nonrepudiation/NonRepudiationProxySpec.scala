// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.KeyPairGenerator

import com.daml.grpc.test.GrpcServer
import com.daml.nonrepudiation.client.{Base64Signature, SigningInterceptor}
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
    val Setup(keys, signatures, proxyBuilder, proxyChannel) = Setup.newInstance
    val keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair()
    keys.put(keyPair.getPublic)

    NonRepudiationProxy
      .owner(
        participant = channel,
        serverBuilder = proxyBuilder,
        keyRepository = keys,
        signedCommandRepository = signatures,
        Health.Name,
      )
      .use { _ =>
        val result =
          Health.getHealthStatus(
            proxyChannel,
            new SigningInterceptor(keyPair, SigningAlgorithm),
          )

        result shouldEqual Health.getHealthStatus(channel)

        inside(signatures.get(Health.Requests.Check.toByteArray)) { case Some(signature) =>
          signature shouldEqual Base64Signature.sign(
            SigningAlgorithm,
            keyPair.getPrivate,
            Health.Requests.Check.toByteArray,
          )
        }

      }
  }

  it should "reject unsigned requests" in withServices(
    Health.newInstance,
    Reflection.newInstance,
  ) { channel =>
    val Setup(keys, signatures, proxyBuilder, proxyChannel) = Setup.newInstance

    NonRepudiationProxy
      .owner(
        participant = channel,
        serverBuilder = proxyBuilder,
        keyRepository = keys,
        signedCommandRepository = signatures,
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
    val Setup(keys, signatures, proxyBuilder, proxyChannel) = Setup.newInstance
    val keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair()

    NonRepudiationProxy
      .owner(channel, proxyBuilder, keys, signatures, Health.Name)
      .use { _ =>
        the[StatusRuntimeException] thrownBy {
          Health.getHealthStatus(
            proxyChannel,
            new SigningInterceptor(keyPair, SigningAlgorithm),
          )
        } should have message SignatureVerificationFailed.asRuntimeException.getMessage
      }
  }

}

object NonRepudiationProxySpec {

  val SigningAlgorithm = "SHA256withRSA"

  final case class Setup(
      keys: KeyRepository,
      signatures: SignedCommandRepository,
      proxyBuilder: InProcessServerBuilder,
      proxyChannel: Channel,
  )

  object Setup {

    def newInstance: Setup = {
      val keys = new KeyRepository.InMemory()
      val signatures = new SignedCommandRepository.InMemory()
      val proxyName = InProcessServerBuilder.generateName()
      val proxyBuilder = InProcessServerBuilder.forName(proxyName)
      val proxyChannel = InProcessChannelBuilder.forName(proxyName).build()
      Setup(keys, signatures, proxyBuilder, proxyChannel)
    }

  }

}
