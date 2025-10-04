// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.protocol.hash.NodeBuilder.NodeEncodingV1
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{Node, NodeId, TransactionVersion}
import com.digitalasset.daml.lf.value.Value

import TransactionHash.*

/** Hash builder with additional methods to encode and hash nodes
  */
private sealed abstract class NodeHashBuilder(
    purpose: HashPurpose,
    hashTracer: HashTracer,
) extends LfValueBuilder(purpose, hashTracer) {

  def addHashingSchemeVersion(hashingSchemeVersion: HashingSchemeVersion): this.type =
    addByte(
      hashingSchemeVersion.index.byteValue,
      byte => s"${formatByteToHexString(byte)} (Hashing Scheme Version)",
    )

  def addNodeEncodingVersion(nodeVersion: Int): this.type =
    addByte(
      nodeVersion.byteValue,
      byte => s"${formatByteToHexString(byte)} (Node Encoding Version)",
    )

  def addMetadataEncodingVersion(metadataVersion: Int): this.type =
    addByte(
      metadataVersion.byteValue,
      byte => s"${formatByteToHexString(byte)} (Metadata Encoding Version)",
    )

  private[hash] def hashNode(
      node: Node,
      nodeSeed: Option[LfHash],
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
      hashTracer: HashTracer = this.hashTracer,
  ): Hash

  @throws[NodeHashingError]
  protected def addNodeFromNodeId(
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
  ): (this.type, NodeId) => this.type =
    (builder, nodeId) => {
      val node = nodes.getOrElse(nodeId, throw NodeHashingError.IncompleteTransactionTree(nodeId))
      addHash(
        builder.hashNode(node, nodeSeeds.get(nodeId), nodes, nodeSeeds, hashTracer.subNodeTracer),
        "(Hashed Inner Node)",
      )
    }

  def addNodesFromNodeIds(
      nodeIds: ImmArray[NodeId],
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
  ): this.type =
    iterateOver(nodeIds)(addNodeFromNodeId(nodes, nodeSeeds))
}

private object NodeBuilder {
  // Version of the protobuf used to encode nodes defined in the interactive_submission_data.proto
  private[hash] val NodeEncodingV1 = 1
  private[hash] val HashingVersionToSupportedLFVersionMapping
      : Map[HashingSchemeVersion, Set[LanguageVersion]] =
    Map(
      HashingSchemeVersion.V2 -> Set(LanguageVersion.v2_1)
    )

  private[hash] sealed abstract class NodeTag(val tag: Byte)

  private[hash] object NodeTag {
    case object CreateTag extends NodeTag(0)
    case object ExerciseTag extends NodeTag(1)
    case object FetchTag extends NodeTag(2)
    case object RollbackTag extends NodeTag(3)
  }

  @throws[NodeHashingError]
  private[hash] def assertHashingVersionSupportsLfVersion(
      version: LanguageVersion,
      nodeHashVersion: HashingSchemeVersion,
  ): Unit =
    if (
      !HashingVersionToSupportedLFVersionMapping
        // This really shouldn't happen, unless someone removed an entry from the HashingVersionToSupportedLFVersionMapping map
        .getOrElse(
          nodeHashVersion,
          throw NodeHashingError.UnsupportedHashingVersion(nodeHashVersion),
        )
        .contains(version)
    ) throw NodeHashingError.UnsupportedLanguageVersion(nodeHashVersion, version)
}

private class NodeBuilderV1(
    purpose: HashPurpose,
    hashTracer: HashTracer,
    enforceNodeSeedForCreateNodes: Boolean,
) extends NodeHashBuilder(purpose, hashTracer) {

  override private[hash] def hashNode(
      node: Node,
      nodeSeed: Option[LfHash],
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
      hashTracer: HashTracer = this.hashTracer,
  ): Hash = {
    node.optVersion
      .foreach(
        NodeBuilder
          .assertHashingVersionSupportsLfVersion(_, HashingSchemeVersion.V2)
      )

    new NodeBuilderV1(purpose, hashTracer, enforceNodeSeedForCreateNodes)
      .addNodeEncodingVersion(NodeEncodingV1)
      .addNode(node, nodeSeed, nodes, nodeSeeds)
      .finish()
  }

  private def addCreateNode(nodeSeed: Option[LfHash]): Node.Create => this.type = {
    // Pattern match to make it more obvious which fields are part of the hashing and which are not
    case Node.Create(
          coid,
          packageName,
          templateId,
          arg,
          signatories,
          stakeholders,
          keyOpt,
          version,
        ) =>
      if (keyOpt.isDefined) notSupported("keyOpt in Create node") // 2.dev feature
      addContext("Create Node")
        .withContext("Node Version")(_.add(TransactionVersion.toProtoValue(version)))
        .addByte(NodeBuilder.NodeTag.CreateTag.tag, _ => "Create Node Tag")
        .withContext("Node Seed")(
          _.addOptional(nodeSeed, builder => seed => builder.addLfHash(seed, "node seed"))
        )
        .withContext("Contract Id")(_.addCid(coid))
        .withContext("Package Name")(_.add(packageName))
        .withContext("Template Id")(_.addIdentifier(templateId))
        .withContext("Arg")(_.addTypedValue(arg))
        .withContext("Signatories")(_.addStringSet(signatories))
        .withContext("Stakeholders")(_.addStringSet(stakeholders))
  }

  private val addFetchNode: Node.Fetch => this.type = {
    case Node.Fetch(
          coid,
          packageName,
          templateId,
          actingParties,
          signatories,
          stakeholders,
          keyOpt,
          byKey,
          interfaceId,
          version,
        ) =>
      if (keyOpt.nonEmpty) notSupported("keyOpt in Fetch node") // 2.dev feature
      if (byKey == true) notSupported("byKey in Fetch node") // 2.dev feature
      addContext("Fetch Node")
        .withContext("Node Version")(_.add(TransactionVersion.toProtoValue(version)))
        .addByte(NodeBuilder.NodeTag.FetchTag.tag, _ => "Fetch Node Tag")
        .withContext("Contract Id")(_.addCid(coid))
        .withContext("Package Name")(_.add(packageName))
        .withContext("Template Id")(_.addIdentifier(templateId))
        .withContext("Signatories")(_.addStringSet(signatories))
        .withContext("Stakeholders")(_.addStringSet(stakeholders))
        .withContext("Interface Id")(_.addOptional(interfaceId, _.addIdentifier))
        .withContext("Acting Parties")(_.addStringSet(actingParties))
  }

  private def addExerciseNode(
      nodes: Map[NodeId, Node],
      nodeSeed: LfHash,
      nodeSeeds: Map[NodeId, LfHash],
  ): Node.Exercise => this.type = {
    case Node.Exercise(
          targetCoid,
          packageName,
          templateId,
          interfaceId,
          choiceId,
          consuming,
          actingParties,
          chosenValue,
          stakeholders,
          signatories,
          choiceObservers,
          choiceAuthorizers,
          children,
          exerciseResult,
          keyOpt,
          byKey,
          version,
        ) =>
      if (choiceAuthorizers.nonEmpty)
        notSupported("choiceAuthorizers in Exercise node") // 2.dev feature
      if (keyOpt.nonEmpty) notSupported("keyOpt in Exercise node") // 2.dev feature
      if (byKey == true) notSupported("byKey in Exercise node") // 2.dev feature
      addContext("Exercise Node")
        .withContext("Node Version")(_.add(TransactionVersion.toProtoValue(version)))
        .addByte(NodeBuilder.NodeTag.ExerciseTag.tag, _ => "Exercise Node Tag")
        .withContext("Node Seed")(_.addLfHash(nodeSeed, "seed"))
        .withContext("Contract Id")(_.addCid(targetCoid))
        .withContext("Package Name")(_.add(packageName))
        .withContext("Template Id")(_.addIdentifier(templateId))
        .withContext("Signatories")(_.addStringSet(signatories))
        .withContext("Stakeholders")(_.addStringSet(stakeholders))
        .withContext("Acting Parties")(_.addStringSet(actingParties))
        .withContext("Interface Id")(_.addOptional(interfaceId, _.addIdentifier))
        .withContext("Choice Id")(_.add(choiceId))
        .withContext("Chosen Value")(_.addTypedValue(chosenValue))
        .withContext("Consuming")(_.addBool(consuming))
        .withContext("Exercise Result")(
          _.addOptional[Value](
            exerciseResult,
            builder => value => builder.addTypedValue(value),
          )
        )
        .withContext("Choice Observers")(_.addStringSet(choiceObservers))
        .withContext("Children")(_.addNodesFromNodeIds(children, nodes, nodeSeeds))
  }

  private def addRollbackNode(
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
  ): Node.Rollback => this.type = { case Node.Rollback(children) =>
    addContext("Rollback Node")
      .addByte(NodeBuilder.NodeTag.RollbackTag.tag, _ => "Rollback Node Tag")
      .withContext("Children")(_.addNodesFromNodeIds(children, nodes, nodeSeeds))
  }

  private def addNode(
      node: Node,
      nodeSeedO: Option[LfHash],
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
  ): this.type = (node, nodeSeedO) match {
    // Create nodes in a transaction should have a node seed, but we also need to encode create nodes for disclosed contracts
    // which do not have one.
    // We could differentiate between the 2 cases but to keep the encoding simpler we encode create nodes with an optional seed
    case (create: Node.Create, nodeSeed) =>
      // We can still enforce that create nodes within a transaction have a seed, even if we then encode it as an optional
      if (enforceNodeSeedForCreateNodes && nodeSeed.isEmpty) missingNodeSeed(node)
      addCreateNode(nodeSeed)(create)
    case (fetch: Node.Fetch, _) => addFetchNode(fetch)
    case (exercise: Node.Exercise, Some(nodeSeed)) =>
      addExerciseNode(nodes, nodeSeed, nodeSeeds)(exercise)
    case (_: Node.Exercise, None) => missingNodeSeed(node)
    case (rollback: Node.Rollback, _) => addRollbackNode(nodes, nodeSeeds)(rollback)
    case (_: Node.LookupByKey, _) =>
      notSupported(s"LookupByKey node")
  }

  private[this] def notSupported(str: String) =
    throw NodeHashingError.UnsupportedFeature(
      s"$str is not supported in version ${HashingSchemeVersion.V2.index}"
    )

  private[this] def missingNodeSeed(node: Node) =
    throw NodeHashingError.MissingNodeSeed(
      s"Missing node seed for node $node"
    )
}
