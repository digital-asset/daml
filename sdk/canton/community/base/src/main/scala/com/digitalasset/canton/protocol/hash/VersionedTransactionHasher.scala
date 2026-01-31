// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.transaction.*

import scala.annotation.unused

object VersionedTransactionHasher {

  /** Deterministically hash a versioned transaction in accordance with the provided hash version.
    */
  @throws[NodeHashingError]
  private[hash] def tryHashTransaction(
      @unused hashVersion: HashingSchemeVersion,
      versionedTransaction: VersionedTransaction,
      nodeSeeds: Map[NodeId, LfHash],
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash =
    new NodeBuilderV1(
      HashPurpose.PreparedSubmission,
      hashTracer,
      enforceNodeSeedForCreateNodes = true,
    ).addPurpose()
      .withContext("Serialization Version")(
        _.addString(SerializationVersion.toProtoValue(versionedTransaction.version))
      )
      .withContext("Root Nodes")(
        _.addNodesFromNodeIds(versionedTransaction.roots, versionedTransaction.nodes, nodeSeeds)
      )
      .finish()

}
