// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance

import com.digitalasset.canton.crypto.provider.jce.JcePrivateCrypto
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeySpec, SigningKeyUsage}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.{
  BftBlockOrderingStandaloneNetworkConfig,
  BftBlockOrderingStandalonePeerConfig,
}
import com.digitalasset.canton.topology.{Namespace, SequencerId}
import pureconfig.ConfigWriter

import scala.util.control.NonFatal

/** Utility to create a standalone BFT block ordering network configuration with the given number of
  * nodes in the specified directory. It generates key pairs for each node and creates a config file
  * for each node referencing the generated keys and the public keys of all other nodes.
  *
  * Usage: `BftBlockOrderingStandaloneNetworkConfig <new directory> <numNodes>`
  */
object GenStandaloneConfig extends App {

  private def printUsageAndExit(): Nothing = {
    println(s"Usage: ${getClass.getSimpleName} <new directory> <numNodes>")
    sys.exit(1)
  }

  private def sequencerId(i: Int): String =
    SequencerId
      .tryCreate(i.toString, Namespace(Fingerprint.tryFromString("default")))
      .toProtoPrimitive

  if (args.sizeIs < 2)
    printUsageAndExit()

  val (dir, numNodes) =
    try {
      better.files.File(args(0)).createDirectory() -> args(1).toInt
    } catch {
      case NonFatal(e) =>
        e.printStackTrace()
        printUsageAndExit()
    }

  for (i <- 1 to numNodes) {
    val keyPair = JcePrivateCrypto
      .generateSigningKeypair(SigningKeySpec.EcCurve25519, SigningKeyUsage.ProtocolOnly)
      .getOrElse(throw new RuntimeException("Failed to generate keypair"))
    val privKey = keyPair.privateKey
    val pubKey = keyPair.publicKey
    val privKeyFile = dir / s"node${i}_signing_private_key.bin"
    val pubKeyFile = dir / s"node${i}_signing_public_key.bin"
    privKeyFile.writeByteArray(privKey.toProtoV30.toByteArray)
    pubKeyFile.writeByteArray(pubKey.toProtoV30.toByteArray)

    val config = BftBlockOrderingStandaloneNetworkConfig(
      thisSequencerId = sequencerId(i),
      signingPrivateKeyProtoFile = privKeyFile.toJava,
      signingPublicKeyProtoFile = pubKeyFile.toJava,
      peers = (1 to numNodes)
        .filter(_ != i)
        .map { j =>
          BftBlockOrderingStandalonePeerConfig(
            sequencerId = sequencerId(j),
            signingPublicKeyProtoFile = dir / s"node${j}_signing_public_key.bin" toJava,
          )
        },
    )
    val configFile = dir / s"node$i.conf"
    import pureconfig.generic.auto.*
    configFile.writeText(
      ConfigWriter[BftBlockOrderingStandaloneNetworkConfig].to(config).render()
    )
  }
}
