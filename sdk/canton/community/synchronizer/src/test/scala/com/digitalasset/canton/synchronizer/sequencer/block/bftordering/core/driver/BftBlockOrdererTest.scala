// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver

import better.files.File
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, Port}
import com.digitalasset.canton.config.{PemFile, PemString, TlsClientConfig, TlsServerConfig}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.{
  P2PEndpointConfig,
  P2PNetworkConfig,
  P2PServerConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time.BftTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
  TlsP2PEndpoint,
}
import org.scalatest.wordspec.AnyWordSpec

class BftBlockOrdererTest extends AnyWordSpec with BaseTest {

  "Config creation" should {

    "fail only" when {
      "there are too many requests per block proposal" in {
        Table(
          ("maxRequestsInBatch", "maxBatchesPerProposal"),
          (31, 31), // 961 < 999
          (999, 1), // 999
          (100, 10), // 1000 > 999
          (32, 32), // 1024 > 999
        ).foreach { case (maxRequestsInBatch, maxBatchesPerProposal) =>
          val maxRequestsPerBlock = maxRequestsInBatch * maxBatchesPerProposal
          if (maxRequestsPerBlock > BftTime.MaxRequestsPerBlock) {
            val expectedMessage =
              s"requirement failed: Maximum block size too big: $maxRequestsInBatch maximum requests per batch and " +
                s"$maxBatchesPerProposal maximum batches per block proposal means " +
                s"$maxRequestsPerBlock maximum requests per block, " +
                s"but the maximum number allowed of requests per block is ${BftTime.MaxRequestsPerBlock}"
            the[IllegalArgumentException] thrownBy BftBlockOrdererConfig(
              maxRequestsInBatch = maxRequestsInBatch.toShort,
              maxBatchesPerBlockProposal = maxBatchesPerProposal.toShort,
            ) should have message expectedMessage
          }
        }
      }
    }

    "allow any combination of server endpoint configuration protocol and external endpoint configuration protocol" in {
      val aTlsServerConfig =
        TlsServerConfig(
          certChainFile = PemString(""),
          privateKeyFile = PemFile(ExistingFile.tryCreate(File.newTemporaryFile().toJava)),
        )
      val aTlsClientConfig =
        TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = true)
      val externalPort = Port.tryCreate(80)
      val expectedHttpEndpoint = PlainTextP2PEndpoint("host", externalPort)
      val expectedTlsEndpoint =
        TlsP2PEndpoint(
          P2PEndpointConfig(
            "host",
            externalPort,
            Some(
              TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = true)
            ),
          )
        )
      Table[Option[TlsServerConfig], Option[TlsClientConfig], P2PEndpoint](
        ("serverTls", "externalTls", "expected server-to-client authentication endpoint"),
        (None, None, expectedHttpEndpoint),
        (Some(aTlsServerConfig), None, expectedHttpEndpoint),
        (None, Some(aTlsClientConfig), expectedTlsEndpoint),
        (Some(aTlsServerConfig), Some(aTlsClientConfig), expectedTlsEndpoint),
      ).foreach { case (serverTls, externalTls, expectedServerToClientAuthenticationEndpoint) =>
        val config =
          BftBlockOrdererConfig(
            initialNetwork = Some(
              P2PNetworkConfig(
                serverEndpoint = P2PServerConfig(
                  address = "localhost",
                  internalPort = Some(Port.tryCreate(10000)),
                  tls = serverTls,
                  externalAddress = "host",
                  externalPort = externalPort,
                  externalTlsConfig = externalTls,
                )
              )
            )
          )
        BftBlockOrderer.getServerToClientAuthenticationEndpoint(
          config
        ) shouldBe Some(expectedServerToClientAuthenticationEndpoint)
      }
    }
  }
}
