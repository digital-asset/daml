// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

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
    final case class UnsupportedLanguageVersion(
        nodeHashVersion: HashingSchemeVersion,
        version: TransactionVersion,
    ) extends NodeHashingError(
          s"Cannot hash node with LF $version using hash version $nodeHashVersion. Supported LF versions: ${NodeBuilder.HashingVersionToSupportedLFVersionMapping
              .getOrElse(nodeHashVersion, Set.empty)
              .mkString(", ")}"
        )

    final case class UnsupportedHashingVersion(version: HashingSchemeVersion)
        extends NodeHashingError(
          s"Cannot hash node with hashing version $version. Supported versions: ${NodeBuilder.HashingVersionToSupportedLFVersionMapping.keySet
              .mkString(", ")}"
        )
  }

  /** Deterministically hash a versioned transaction and its metadata using the Version 1 of the hashing algorithm.
    *
    * @param hashTracer tracer that can be used to debug encoding of the transaction.
    */
  @throws[NodeHashingError]
  def tryHashTransactionWithMetadataV1(
      versionedTransaction: VersionedTransaction,
      nodeSeeds: Map[NodeId, LfHash],
      metadata: TransactionMetadataHashBuilder.MetadataV1,
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash =
    new NodeBuilderV1(
      HashPurpose.PreparedSubmission,
      hashTracer,
      enforceNodeSeedForCreateNodes = true,
    ).addPurpose
      .addHash(
        TransactionHash
          .tryHashTransactionV1(versionedTransaction, nodeSeeds, hashTracer.subNodeTracer),
        "Transaction",
      )
      .addHash(
        TransactionMetadataHashBuilder
          .hashTransactionMetadataV1(metadata, hashTracer.subNodeTracer),
        "Metadata",
      )
      .finish()

  /** Deterministically hash a versioned transaction using the Version 1 of the hashing algorithm.
    *
    * @param hashTracer tracer that can be used to debug encoding of the transaction.
    */
  @throws[NodeHashingError]
  private[hash] def tryHashTransactionV1(
      versionedTransaction: VersionedTransaction,
      nodeSeeds: Map[NodeId, LfHash],
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash =
    new NodeBuilderV1(
      HashPurpose.PreparedSubmission,
      hashTracer,
      enforceNodeSeedForCreateNodes = true,
    ).addPurpose
      .withContext("Transaction Version")(
        _.add(TransactionVersion.toProtoValue(versionedTransaction.version))
      )
      .withContext("Root Nodes")(
        _.addNodesFromNodeIds(versionedTransaction.roots, versionedTransaction.nodes, nodeSeeds)
      )
      .finish()

  // For testing only
  @throws[NodeHashingError]
  private[hash] def tryHashNodeV1(
      node: Node,
      nodeSeeds: Map[NodeId, LfHash] = Map.empty,
      nodeSeed: Option[LfHash] = None,
      subNodes: Map[NodeId, Node] = Map.empty,
      hashTracer: HashTracer = HashTracer.NoOp,
      enforceNodeSeedForCreateNodes: Boolean = true,
  ): Hash =
    new NodeBuilderV1(HashPurpose.PreparedSubmission, hashTracer, enforceNodeSeedForCreateNodes)
      .hashNode(node, nodeSeed, subNodes, nodeSeeds)
}
