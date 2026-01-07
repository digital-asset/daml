// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.standalone.topology

import better.files.File as BFile
import com.digitalasset.canton.crypto.{CryptoPureApi, SigningPrivateKey, SigningPublicKey, v30}
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.standalone.crypto.FixedKeysCryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.{
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftKeyId,
  BftNodeId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MaxBytesToDecompress

import java.io.File
import scala.concurrent.ExecutionContext

class FixedFileBasedOrderingTopologyProvider(
    standaloneConfig: BftBlockOrdererConfig.BftBlockOrderingStandaloneNetworkConfig,
    crypto: CryptoPureApi,
    metrics: BftOrderingMetrics,
)(implicit executionContext: ExecutionContext)
    extends OrderingTopologyProvider[PekkoEnv] {

  private val pubKey = readSigningPublicKey(standaloneConfig.signingPublicKeyProtoFile)

  private val privKey =
    SigningPrivateKey
      .fromProtoV30(
        v30.SigningPrivateKey
          .parseFrom(BFile(standaloneConfig.signingPrivateKeyProtoFile.getAbsolutePath).byteArray)
      )
      .getOrElse(throw new IllegalArgumentException("Failed to parse signing private key"))

  private val peerSigningPublicKeys =
    standaloneConfig.peers.map { peerConfig =>
      BftNodeId(peerConfig.sequencerId) ->
        readSigningPublicKey(peerConfig.signingPublicKeyProtoFile)
    }.toMap + (BftNodeId(standaloneConfig.thisSequencerId) -> pubKey)

  private val orderingTopology =
    OrderingTopology(
      Map(
        BftNodeId(standaloneConfig.thisSequencerId) ->
          OrderingTopology.NodeTopologyInfo(
            Genesis.GenesisTopologyActivationTime,
            Set(BftKeyId(pubKey.fingerprint.toProtoPrimitive)),
          )
      ) ++ standaloneConfig.peers.map { peerConfig =>
        BftNodeId(peerConfig.sequencerId) ->
          OrderingTopology.NodeTopologyInfo(
            Genesis.GenesisTopologyActivationTime,
            Set(
              BftKeyId(
                peerSigningPublicKeys(
                  BftNodeId(peerConfig.sequencerId)
                ).fingerprint.toProtoPrimitive
              )
            ),
          )
      },
      SequencingParameters.Default,
      MaxBytesToDecompress(DynamicSynchronizerParameters.defaultMaxRequestSize.value),
      Genesis.GenesisTopologyActivationTime,
      areTherePendingCantonTopologyChanges = false,
    )

  override def getOrderingTopologyAt(activationTime: TopologyActivationTime)(implicit
      traceContext: TraceContext
  ): PekkoEnv#FutureUnlessShutdownT[Option[(OrderingTopology, CryptoProvider[PekkoEnv])]] =
    PekkoFutureUnlessShutdown.pure(
      Some(
        (
          orderingTopology,
          new FixedKeysCryptoProvider(privKey, peerSigningPublicKeys, crypto, metrics),
        )
      )
    )

  private def readSigningPublicKey(signingPublicKeyProtoFile: File) =
    SigningPublicKey
      .fromProtoV30(
        v30.SigningPublicKey.parseFrom(
          BFile(signingPublicKeyProtoFile.getAbsolutePath).byteArray
        )
      )
      .getOrElse(
        throw new IllegalArgumentException("Failed to parse signing public key")
      )
}
