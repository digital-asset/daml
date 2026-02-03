// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.InteractiveSubmission.TransactionMetadataForHashing
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.transaction.*

object TransactionHash {
  sealed abstract class NodeHashingError(val msg: String) extends Exception(msg)
  object NodeHashingError {
    final case class UnsupportedFeature(message: String) extends NodeHashingError(message)
    final case class MissingNodeSeed(message: String) extends NodeHashingError(message)
    final case class IncompleteTransactionTree(nodeId: NodeId)
        extends NodeHashingError(s"The transaction does not contain a node with nodeId $nodeId")
    final case class UnsupportedSerializationVersion(
        nodeHashVersion: HashingSchemeVersion,
        version: SerializationVersion,
    ) extends NodeHashingError(
          s"Cannot hash node with LF $version using hash version $nodeHashVersion. Supported LF serialization versions: ${NodeBuilder.HashingVersionToSupportedLFSerializationVersionMapping
              .getOrElse(nodeHashVersion, Set.empty)
              .mkString(", ")}"
        )
    final case class UnsupportedHashingVersion(version: HashingSchemeVersion)
        extends NodeHashingError(
          s"Cannot hash node with hashing version $version. Supported versions: ${NodeBuilder.HashingVersionToSupportedLFSerializationVersionMapping.keySet
              .mkString(", ")}"
        )
  }

  /** Deterministically hash a versioned transaction and its metadata using the Version 1 of the
    * hashing algorithm.
    *
    * @param hashTracer
    *   tracer that can be used to debug encoding of the transaction.
    */
  @throws[NodeHashingError]
  def tryHashTransactionWithMetadata(
      hashVersion: HashingSchemeVersion,
      versionedTransaction: VersionedTransaction,
      nodeSeeds: Map[NodeId, LfHash],
      metadata: TransactionMetadataForHashing,
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash =
    new NodeBuilderV1(
      HashPurpose.PreparedSubmission,
      hashTracer,
      enforceNodeSeedForCreateNodes = true,
    ).addPurpose()
      .addHashingSchemeVersion(HashingSchemeVersion.V2)
      .addHash(
        VersionedTransactionHasher.tryHashTransaction(
          hashVersion,
          versionedTransaction,
          nodeSeeds,
          hashTracer.subNodeTracer,
        ),
        "Transaction",
      )
      .addHash(
        TransactionMetadataHasher.tryHashMetadata(hashVersion, metadata, hashTracer.subNodeTracer),
        "Metadata",
      )
      .finish()

}
