// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.daml.crypto.{MacPrototype, MessageDigestPrototype}

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import com.digitalasset.daml.lf.data.{
  Bytes,
  FrontStack,
  ImmArray,
  Ref,
  SortedLookupList,
  Time,
  Utf8,
}
import com.digitalasset.daml.lf.value.Value
import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.HashUtils.{HashTracer, formatByteToHexString}
import com.digitalasset.daml.lf.data.Ref.Name
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value.ContractId
import scalaz.Order

import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.collection.immutable.{SortedMap, SortedSet}
import scala.util.control.NoStackTrace

final class Hash private (val bytes: Bytes) {

  override def hashCode(): Int = bytes.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case that: Hash => this.bytes equals that.bytes
    case _ => false
  }

  def toHexString: Ref.HexString = Ref.HexString.encode(bytes)

  override def toString: String = s"Hash($toHexString)"
}

object Hash {

  val underlyingHashLength = 32

  sealed abstract class HashingError(val msg: String) extends Exception with NoStackTrace
  object HashingError {
    final case class ForbiddenContractId()
        extends HashingError("Contract IDs are not supported in contract keys.")
  }

  sealed abstract class NodeHashingError(val msg: String) extends Exception
  object NodeHashingError {
    final case class UnsupportedFeature(message: String) extends NodeHashingError(message)
    final case class MissingNodeSeed(message: String) extends NodeHashingError(message)
    final case class IncompleteTransactionTree(nodeId: NodeId)
        extends NodeHashingError(s"The transaction does not contain a node with nodeId $nodeId")
    final case class UnsupportedLanguageVersion(
        nodeHashVersion: NodeHashVersion,
        version: TransactionVersion,
    ) extends NodeHashingError(
          s"Cannot hash node with LF $version using hash version $nodeHashVersion. Supported LF versions: ${NodeBuilder.HashingVersionToSupportedLFVersionMapping
              .getOrElse(nodeHashVersion, Set.empty)
              .mkString(", ")}"
        )
    final case class UnsupportedHashingVersion(version: NodeHashVersion)
        extends NodeHashingError(
          s"Cannot hash node with hashing version $version. Supported versions: ${NodeBuilder.HashingVersionToSupportedLFVersionMapping.keySet
              .mkString(", ")}"
        )
  }

  private def handleError[X](x: => X): Either[HashingError, X] =
    try {
      Right(x)
    } catch {
      case e: HashingError => Left(e)
    }

  def fromBytes(bs: Bytes): Either[String, Hash] =
    Either.cond(
      bs.length == underlyingHashLength,
      new Hash(bs),
      s"hash should have ${underlyingHashLength} bytes, got ${bs.length}",
    )

  def assertFromBytes(bs: Bytes): Hash =
    data.assertRight(fromBytes(bs))

  def fromByteArray(a: Array[Byte]): Either[String, Hash] =
    fromBytes(Bytes.fromByteArray(a))

  def assertFromByteArray(a: Array[Byte]): Hash =
    data.assertRight(fromByteArray(a))

  // A cryptographic pseudo random generator of Hashes based on hmac
  // Must be given a high entropy seed when used in production.
  // Thread safe
  def secureRandom(seed: Hash): () => Hash = {
    val counter = new AtomicLong
    () => hMacBuilder(seed).addLong(counter.getAndIncrement()).build
  }

  implicit val ordering: Ordering[Hash] =
    Ordering.by(_.bytes)

  implicit val order: Order[Hash] = Order.fromScalaOrdering

  private[lf] val aCid2Bytes: Value.ContractId => Bytes = { case cid @ Value.ContractId.V1(_, _) =>
    cid.toBytes
  }

  private[lf] val noCid2String: Value.ContractId => Nothing =
    _ => throw HashingError.ForbiddenContractId()

  private[lf] val bigIntNumericToBytes: data.Numeric => Bytes =
    numeric => Bytes.fromByteArray(numeric.unscaledValue().toByteArray)

  private[lf] val stringNumericToBytes: data.Numeric => Bytes =
    numeric => Utf8.getBytes(data.Numeric.toString(numeric))

  private[crypto] sealed abstract class Builder(
      numericToBytes: data.Numeric => Bytes
  ) {

    /** @param context Used by `HashTracer`s to provide contextualized information about what the encoded value represents
      */
    protected def update(a: ByteBuffer, context: => String): Unit

    protected def update(a: Array[Byte], context: => String): Unit

    protected def doFinal(buf: Array[Byte]): Unit

    final def build: Hash = {
      val a = Array.ofDim[Byte](underlyingHashLength)
      doFinal(a)
      new Hash(Bytes.fromByteArray(a))
    }

    /* add size delimited byte array. */
    def addBytes(bytes: Array[Byte], context: => String): this.type = {
      addInt(bytes.length).update(bytes, context)
      this
    }

    /* add size delimited byte string. */
    def addBytes(bytes: Bytes, context: => String): this.type = {
      addInt(bytes.length).update(bytes.toByteBuffer, context)
      this
    }

    private val byteBuff = Array.ofDim[Byte](1)

    final def addByte(a: Byte, context: => String): this.type = {
      byteBuff(0) = a
      update(byteBuff, context)
      this
    }

    /* no size delimitation as hashes have fixed size  */
    final def addHash(a: Hash, context: => String): this.type = {
      update(a.bytes.toByteBuffer, context)
      this
    }

    /* add size delimited utf8 string. */
    final def addString(s: String): this.type =
      addBytes(Utf8.getBytes(s), s"$s (string)")

    final def addBool(b: Boolean): this.type =
      addByte(if (b) 1.toByte else 0.toByte, s"${b.toString} (bool)")

    private val intBuffer = ByteBuffer.allocate(java.lang.Integer.BYTES)

    final def addInt(a: Int): this.type = {
      discard(intBuffer.rewind())
      discard(intBuffer.putInt(a).position(0))
      update(intBuffer, s"${a.toString} (int)")
      this
    }

    private val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    final def addLong(a: Long): this.type = {
      discard(longBuffer.rewind())
      discard(longBuffer.putLong(a).position(0))
      update(longBuffer, s"${a.toString} (long)")
      this
    }

    final def addNumeric(v: data.Numeric): this.type =
      addBytes(numericToBytes(v), s"${data.Numeric.toString(v)} (numeric)")

    final def iterateOver[T, U](a: ImmArray[T])(f: (this.type, T) => this.type): this.type =
      a.foldLeft[this.type](addInt(a.length))(f)

    final def iterateOver[T](a: Iterator[T], size: Int)(f: (this.type, T) => this.type): this.type =
      a.foldLeft[this.type](addInt(size))(f)

    final def addDottedName(name: Ref.DottedName): this.type =
      iterateOver(name.segments)(_ addString _)

    final def addQualifiedName(name: Ref.QualifiedName): this.type =
      addDottedName(name.module).addDottedName(name.name)

    final def addIdentifier(id: Ref.Identifier): this.type =
      addString(id.packageId).addQualifiedName(id.qualifiedName)

    final def addStringSet[S <: String](set: Set[S]): this.type = {
      val ss = set.toSeq.sorted[String]
      iterateOver(ss.iterator, ss.size)(_ addString _)
    }

    final def addOptional[S](opt: Option[S], hashS: this.type => S => this.type): this.type = {
      opt match {
        case None => addByte(0.toByte, "None")
        case Some(value) => hashS(addByte(1.toByte, "Some"))(value)
      }
    }
  }

  object TransactionMetadataBuilderV1 {
    final case class Metadata(
        actAs: SortedSet[Ref.Party],
        commandId: Ref.CommandId,
        transactionUUID: UUID,
        mediatorGroup: Int,
        domainId: String,
        ledgerEffectiveTime: Option[Time.Timestamp],
        submissionTime: Time.Timestamp,
        disclosedContracts: SortedMap[ContractId, FatContractInstance],
    )
  }

  /** Hashes Transaction Metadata using the V1 Hashing Scheme
    */
  @throws[NodeHashingError]
  @throws[HashingError]
  def hashTransactionMetadataV1(
      metadata: TransactionMetadataBuilderV1.Metadata,
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash = {
    // Do not enforce node seed for create nodes here as we hash disclosed events which do not have a seed
    new NodeBuilderV1(Purpose.MetadataHash, hashTracer, enforceNodeSeedForCreateNodes = false)
      .addHashVersion(NodeHashVersion.V1)
      .withContext("Act As Parties")(
        _.iterateOver(metadata.actAs.iterator, metadata.actAs.size)(_ addString _)
      )
      .withContext("Command Id")(_.addString(metadata.commandId))
      .withContext("Transaction UUID")(_.addString(metadata.transactionUUID.toString))
      .withContext("Mediator Group")(_.addInt(metadata.mediatorGroup))
      .withContext("Domain Id")(_.addString(metadata.domainId))
      .withContext("Ledger Effective Time")(
        _.addOptional(metadata.ledgerEffectiveTime.map(_.micros), _.addLong)
      )
      .withContext("Submission Time")(_.addLong(metadata.submissionTime.micros))
      .withContext("Disclosed Contracts")(
        _.iterateOver(metadata.disclosedContracts.valuesIterator, metadata.disclosedContracts.size)(
          (builder, fatInstance) =>
            builder
              .withContext("Created At")(_.addLong(fatInstance.createdAt.micros))
              .withContext("Create Contract")(builder =>
                builder.addHash(
                  builder.hashNode(
                    node = fatInstance.toCreateNode,
                    nodeSeed = Option.empty[Hash],
                    nodes = Map.empty,
                    nodeSeeds = Map.empty,
                    hashTracer = hashTracer.subNodeTracer,
                  ),
                  "Disclosed Contract",
                )
              )
        )
      )
      .build
  }

  /** Class with additional methods to hash nodes. Uses a single MessageDigest to hash the entire node including all its values.
    */
  private sealed abstract class NodeBuilder(
      purpose: Purpose,
      hashTracer: HashTracer,
  ) extends LegacyBuilder(purpose, aCid2Bytes, stringNumericToBytes, hashTracer) {

    def addHashVersion(version: NodeHashVersion): this.type = {
      addByte(version.id, s"${formatByteToHexString(version.id)} (Node Encoding Version)")
    }

    private[crypto] def hashNode(
        node: Node,
        nodeSeed: Option[Hash],
        nodes: Map[NodeId, Node],
        nodeSeeds: Map[NodeId, Hash],
        hashTracer: HashTracer = this.hashTracer,
    ): Hash

    @throws[NodeHashingError]
    protected def addNodeFromNodeId(
        nodes: Map[NodeId, Node],
        nodeSeeds: Map[NodeId, Hash],
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
        nodeSeeds: Map[NodeId, Hash],
    ): this.type =
      iterateOver(nodeIds)(addNodeFromNodeId(nodes, nodeSeeds))
  }

  private object NodeBuilder {
    private[lf] val HashingVersionToSupportedLFVersionMapping
        : Map[NodeHashVersion, Set[LanguageVersion]] =
      Map(
        NodeHashVersion.V1 -> Set(LanguageVersion.v2_1)
      )

    private[crypto] sealed abstract class NodeTag(val tag: Byte)
    private[crypto] object NodeTag {
      case object CreateTag extends NodeTag(0)
      case object ExerciseTag extends NodeTag(1)
      case object FetchTag extends NodeTag(2)
      case object LookupTag extends NodeTag(3)
      case object RollbackTag extends NodeTag(4)
    }

    @throws[NodeHashingError]
    private[lf] def assertHashingVersionSupportsLfVersion(
        version: LanguageVersion,
        nodeHashVersion: NodeHashVersion,
    ): Unit = {
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
  }

  private sealed class NodeBuilderV1(
      purpose: Purpose,
      hashTracer: HashTracer,
      enforceNodeSeedForCreateNodes: Boolean,
  ) extends NodeBuilder(purpose, hashTracer) {

    override private[crypto] def hashNode(
        node: Node,
        nodeSeed: Option[Hash],
        nodes: Map[NodeId, Node],
        nodeSeeds: Map[NodeId, Hash],
        hashTracer: HashTracer = this.hashTracer,
    ): Hash = {
      node.optVersion
        .foreach(
          NodeBuilder
            .assertHashingVersionSupportsLfVersion(_, NodeHashVersion.V1)
        )

      new NodeBuilderV1(purpose, hashTracer, enforceNodeSeedForCreateNodes).addVersion
        .addHashVersion(NodeHashVersion.V1)
        .addNode(node, nodeSeed, nodes, nodeSeeds)
        .build
    }

    private def addCreateNode(nodeSeed: Option[Hash]): Node.Create => this.type = {
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
          .withContext("Node Version")(_.addString(TransactionVersion.toProtoValue(version)))
          .addByte(NodeBuilder.NodeTag.CreateTag.tag, "Node Tag")
          .withContext("Node Seed")(
            _.addOptional(nodeSeed, builder => seed => builder.addHash(seed, "node seed"))
          )
          .withContext("Contract Id")(_.addCid(coid))
          .withContext("Package Name")(_.addString(packageName))
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
        if (interfaceId.nonEmpty) notSupported("interfaceId in Fetch node") // 2.dev feature
        addContext("Fetch Node")
          .withContext("Node Version")(_.addString(TransactionVersion.toProtoValue(version)))
          .addByte(NodeBuilder.NodeTag.FetchTag.tag, "Node Tag")
          .withContext("Contract Id")(_.addCid(coid))
          .withContext("Package Name")(_.addString(packageName))
          .withContext("Template Id")(_.addIdentifier(templateId))
          .withContext("Signatories")(_.addStringSet(signatories))
          .withContext("Stakeholders")(_.addStringSet(stakeholders))
          .withContext("Acting Parties")(_.addStringSet(actingParties))
    }

    private def addExerciseNode(
        nodes: Map[NodeId, Node],
        nodeSeed: Hash,
        nodeSeeds: Map[NodeId, Hash],
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
          .withContext("Node Version")(_.addString(TransactionVersion.toProtoValue(version)))
          .addByte(NodeBuilder.NodeTag.ExerciseTag.tag, "Node Tag")
          .withContext("Node Seed")(_.addHash(nodeSeed, "seed"))
          .withContext("Contract Id")(_.addCid(targetCoid))
          .withContext("Package Name")(_.addString(packageName))
          .withContext("Template Id")(_.addIdentifier(templateId))
          .withContext("Signatories")(_.addStringSet(signatories))
          .withContext("Stakeholders")(_.addStringSet(stakeholders))
          .withContext("Acting Parties")(_.addStringSet(actingParties))
          .withContext("Interface Id")(_.addOptional(interfaceId, _.addIdentifier))
          .withContext("Choice Id")(_.addString(choiceId))
          .withContext("Chosen Value")(_.addTypedValue(chosenValue))
          .withContext("Consuming")(_.addBool(consuming))
          .withContext("Exercise Result")(
            _.addOptional[Value](
              exerciseResult,
              { builder => value => builder.addTypedValue(value) },
            )
          )
          .withContext("Choice Observers")(_.addStringSet(choiceObservers))
          .withContext("Children")(_.addNodesFromNodeIds(children, nodes, nodeSeeds))
    }

    private def addRollbackNode(
        nodes: Map[NodeId, Node],
        nodeSeeds: Map[NodeId, Hash],
    ): Node.Rollback => this.type = { case Node.Rollback(children) =>
      addContext("Rollback Node")
        .addByte(NodeBuilder.NodeTag.RollbackTag.tag, "Node Tag")
        .withContext("Children")(_.addNodesFromNodeIds(children, nodes, nodeSeeds))
    }

    private def addNode(
        node: Node,
        nodeSeedO: Option[Hash],
        nodes: Map[NodeId, Node],
        nodeSeeds: Map[NodeId, Hash],
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
        s"$str is not supported in version ${NodeHashVersion.V1.id}"
      )

    private[this] def missingNodeSeed(node: Node) =
      throw NodeHashingError.MissingNodeSeed(
        s"Missing node seed for node $node"
      )
  }

  /** Deterministically hash a versioned transaction using the Version 1 of the hashing algorithm.
    * @param hashTracer tracer that can be used to debug encoding of the transaction.
    */
  @throws[NodeHashingError]
  @throws[HashingError]
  def hashTransactionV1(
      versionedTransaction: VersionedTransaction,
      nodeSeeds: Map[NodeId, Hash],
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash = {
    new NodeBuilderV1(Purpose.TransactionHash, hashTracer, enforceNodeSeedForCreateNodes = true)
      .withContext("Transaction Version")(
        _.addString(TransactionVersion.toProtoValue(versionedTransaction.version))
      )
      .withContext("Root Nodes")(
        _.addNodesFromNodeIds(versionedTransaction.roots, versionedTransaction.nodes, nodeSeeds)
      )
      .build
  }

  /** Deterministically hash a node using the Version 1 of the hashing algorithm.
    * @param hashTracer tracer that can be used to debug encoding of the node.
    */
  @throws[NodeHashingError]
  @throws[HashingError]
  def hashNodeV1(
      node: Node,
      nodeSeeds: Map[NodeId, Hash] = Map.empty,
      nodeSeed: Option[Hash] = None,
      subNodes: Map[NodeId, Node] = Map.empty,
      hashTracer: HashTracer = HashTracer.NoOp,
      enforceNodeSeedForCreateNodes: Boolean = true,
  ): Hash = {
    new NodeBuilderV1(Purpose.TransactionHash, hashTracer, enforceNodeSeedForCreateNodes)
      .hashNode(node, nodeSeed, subNodes, nodeSeeds)
  }

  // Only for testing
  private[crypto] def valueBuilderForV1Node(
      hashTracer: HashTracer = HashTracer.NoOp
  ): ValueHashBuilder =
    new NodeBuilderV1(Purpose.TransactionHash, hashTracer, enforceNodeSeedForCreateNodes = false)

  private final class HashMacBuilder(key: Hash) extends Builder(bigIntNumericToBytes) {
    private val macPrototype: MacPrototype = MacPrototype.HmacSha256
    private val mac: Mac = macPrototype.newMac

    mac.init(new SecretKeySpec(key.bytes.toByteArray, macPrototype.algorithm))

    final override protected def update(a: ByteBuffer, context: => String): Unit =
      mac.update(a)

    final override protected def update(a: Array[Byte], context: => String): Unit =
      mac.update(a)

    final override protected def doFinal(buf: Array[Byte]): Unit =
      mac.doFinal(buf, 0)
  }

  private[crypto] sealed abstract class ValueHashBuilder(
      version: Version,
      purpose: Purpose,
      cid2Bytes: Value.ContractId => Bytes,
      numericToBytes: data.Numeric => Bytes,
      hashTracer: HashTracer,
  ) extends Builder(numericToBytes) {

    protected val md = MessageDigestPrototype.Sha256.newDigest

    def addContext(context: => String): this.type = {
      withContext(context)(identity)
    }

    def withContext(context: => String)(f: this.type => this.type): this.type = {
      hashTracer.context(s"# $context")
      f(this)
    }

    /*
     * In order to avoid hash collision, this should be used together
     *  with another data representing uniquely the type of `value`.
     * See for instance hash : Node.GlobalKey => SHA256Hash
     */

    @throws[HashingError]
    def addTypedValue(value: Value): this.type

    def addCid(cid: Value.ContractId): this.type =
      addBytes(cid2Bytes(cid), s"${cid.coid} (contractId)")

    override protected def update(a: ByteBuffer, context: => String): Unit = {
      hashTracer.trace(a, context)
      md.update(a)
    }

    override protected def update(a: Array[Byte], context: => String): Unit = {
      hashTracer.trace(a, context)
      md.update(a)
    }

    override protected def doFinal(buf: Array[Byte]): Unit =
      assert(md.digest(buf, 0, underlyingHashLength) == underlyingHashLength)

    def addVersion: this.type = {
      addByte(version.id, s"${formatByteToHexString(version.id)} (Value Encoding Version)")
        .addByte(purpose.id, s"${formatByteToHexString(purpose.id)} (Value Encoding Purpose)")
    }
  }

  // TODO #20203 Rename to a better suited name
  private[crypto] sealed class LegacyBuilder(
      purpose: Purpose,
      cid2Bytes: Value.ContractId => Bytes,
      numericToBytes: data.Numeric => Bytes,
      hashTracer: HashTracer,
  ) extends ValueHashBuilder(
        version = Version.Legacy,
        purpose = purpose,
        cid2Bytes = cid2Bytes,
        numericToBytes = numericToBytes,
        hashTracer = hashTracer,
      ) {

    /*
     * In the legacy case (i.e. contract cannot be upgraded) we implemented  following collision resistance property:
     *
     * Given two Daml values `v1` and `v2` of the same ground type
     * `T`, i.e., no type variables in T, if `hash(v1) == hash(v2)`,
     * then either `v1 == v2` or we have found a hash collision in the
     * underlying hash function.

     * Given the construction as a plain hash of bytes, this
     * equivalently means that `v1` and `v2`must serialize to
     * different bytes if `v1 != v2`. In fact, for the encoding for
     * compound types like lists and records to work, we require that
     * the encoding of values of the same type is prefix-free, i.e.,
     * `v1` and `v2`'s encodings are not prefixes of each other.
     *
     * In particular, collections require that values they contain are
     * never encoded as an empty bytestring.
     *
     */
    final override def addTypedValue(value: Value): this.type =
      value match {
        case Value.ValueUnit =>
          addByte(0.toByte, "00 (unit)")
        case Value.ValueBool(b) =>
          addBool(b)
        case Value.ValueInt64(v) =>
          addLong(v)
        case Value.ValueNumeric(v) =>
          addNumeric(v)
        case Value.ValueTimestamp(v) =>
          addLong(v.micros)
        case Value.ValueDate(v) =>
          addInt(v.days)
        case Value.ValueParty(v) =>
          addString(v)
        case Value.ValueText(v) =>
          addString(v)
        case Value.ValueContractId(cid) =>
          addBytes(cid2Bytes(cid), s"${cid.coid} (contractId)")
        case Value.ValueOptional(opt) =>
          // We use Int instead of Byte for indicating Some vs None.
          // This waists 3 unnecessary bytes, but we have to keep it for backward compatibility.
          opt match {
            case Some(value) =>
              addInt(1).addTypedValue(value)
            case None =>
              addInt(0)
          }
        case Value.ValueList(xs) =>
          iterateOver(xs.toImmArray)(_ addTypedValue _)
        case Value.ValueTextMap(xs) =>
          iterateOver(xs.toImmArray)((acc, x) => acc.addString(x._1).addTypedValue(x._2))
        case Value.ValueRecord(_, fs) =>
          iterateOver(fs)(_ addTypedValue _._2)
        case Value.ValueVariant(_, variant, v) =>
          addString(variant).addTypedValue(v)
        case Value.ValueEnum(_, v) =>
          addString(v)
        case Value.ValueGenMap(entries) =>
          iterateOver(entries.iterator, entries.length)((acc, x) =>
            acc.addTypedValue(x._1).addTypedValue(x._2)
          )
      }
  }

  private final class UpgradeFriendlyBuilder(
      purpose: Purpose,
      cid2Bytes: Value.ContractId => Bytes,
      numericToBytes: data.Numeric => Bytes,
      hashTracer: HashTracer,
  ) extends ValueHashBuilder(
        version = Version.UpgradeFriendly,
        purpose = purpose,
        cid2Bytes = cid2Bytes,
        numericToBytes = numericToBytes,
        hashTracer = hashTracer,
      ) {

    /*
     * With upgrading, the restriction that values of the same type
     * (from the legacy case) is too strong. We want the following
     * relax property:
     *
     * Given two Daml values `v1` of type `T1` and `v2` of type `T2`
     * such that there is a type `T` so that `v1` can be up/downgraded
     * into `v1'` of type `T` and `v2` can be up/downgraded into `v2'`
     * of type `T`, then `hash(v1) == hash(v2)` implies that `v1 ==
     * v2` or we have found a hash collision in the underlying hash
     * function.

     *  For example, let `T1 = {1: Int, i: Option[Time]}` and `T2 =
     *  {1: Int, j: Option[Long]}` with `v1 = {1 = 5, i = None} : T1`
     *  and `v2 = {1 = 6, j = None}` for some field numbers `i` and
     *  `j` greater than 1. Then `v1` and `v2` can be downgraded to `T
     *  = {1: Int}` and so we don't want their hashes to be the
     *  same. Conversely, they can both be upgraded to `T' = {1: Int,
     *  i: Option[Time], j: Option[Long]}` if `i != j` and so their
     *  hashes must be different for this reason to, even if the
     *  fields `i` and `j` are not None. For `i = j`, however, there
     *  is no such `T'`. So it is fine that `v1' = {1 = 5, i:
     *  Some(Epoch)} : T1` and `v2' = {1 = 5, j = Some(0L)} : T2` have
     *  same hash because there is no type that contains both `v1'` and
     *  `v2'`.
     *
     * To achieve that, the new "friendly upgrade" scheme introduces
     * notion of default values, and ignores record fields with
     * default value, so they do not contribute to the value hash.
     *
     * For the sake of extensibility we decided to introduce default
     * value for more most of the scala types, text and builtin
     * collections, instead of only optional. Concretely for
     *  - scala types (default if equal to 0)
     *  - text (default if empty)
     *  - collections: optional, list, maps (default if empty).
     *
     * On the other hand, user data types (in particular records) have
     * no default value -- we could have consider a record with only
     * field with default values, default itself. We decided to not go
     * this way, as it is much more complicate to implement (we would
     * need to recursively inspect its fields before be able to
     * declare it default).
     *
     * Following inspiration from protobuf, record field encoding
     * prefixed with their filed numbers if they are not default,
     * otherwise they are ignored. Note the encoding for records
     * remains prefix-free (assuming that the field contents'
     * encodings are prefix-free) because we add the the end
     * terminator 0xFF which is not a prefix of any field number
     * (field number are positive integer)
     */

    final override def addTypedValue(value: Value): this.type =
      value match {
        case Value.ValueUnit =>
          // We could use value.productPrefix to enrich the context here and for all values
          addByte(0.toByte, "0 (unit)")
        case Value.ValueBool(b) =>
          addBool(b)
        case Value.ValueInt64(v) =>
          addLong(v)
        case Value.ValueDate(v) =>
          addInt(v.days)
        case Value.ValueTimestamp(v) =>
          addLong(v.micros)
        case Value.ValueNumeric(v) =>
          addNumeric(v)
        case Value.ValueParty(v) =>
          addString(v)
        case Value.ValueText(v) =>
          addString(v)
        case Value.ValueContractId(cid) =>
          addCid(cid)
        case Value.ValueOptional(opt) =>
          opt match {
            case Some(value) => addByte(1.toByte, "Some (optional)").addTypedValue(value)
            case None => addByte(0.toByte, "None (optional)")
          }
        case Value.ValueList(xs) =>
          addList(xs)
        case Value.ValueTextMap(xs) =>
          addTextMap(xs)
        case Value.ValueRecord(_, fs) =>
          addRecord(fs)
        case Value.ValueVariant(_, variant, v) =>
          addVariant(variant, v)
        case Value.ValueEnum(_, v) =>
          addString(v)
        case Value.ValueGenMap(entries) =>
          addGenMap(entries)
      }

    private def addGenMap(entries: ImmArray[(Value, Value)]): this.type =
      iterateOver(entries.iterator, entries.length)((acc, x) =>
        acc.addTypedValue(x._1).addTypedValue(x._2)
      )

    private def addVariant(variant: Name, v: Value): this.type =
      addString(variant).addTypedValue(v)

    private def addTextMap(xs: SortedLookupList[Value]): this.type =
      iterateOver(xs.toImmArray)((acc, x) => acc.addString(x._1).addTypedValue(x._2))

    private def addList(xs: FrontStack[Value]): this.type =
      iterateOver(xs.toImmArray)(_ addTypedValue _)

    // we add non-default fields together with their 1-based field index,
    // we end using 0 delimiter.
    def addRecord(value: ImmArray[(_, Value)]): this.type = {
      value.iterator.zipWithIndex.foreach[Unit] { case ((_, value), i) =>
        def addField: this.type = addInt(i)
        value match {
          case leaf: Value.ValueCidlessLeaf =>
            leaf match {
              case Value.ValueEnum(_, value) =>
                // No default value for enum
                discard(addField.addString(value))
              case Value.ValueInt64(value) =>
                if (value != 0)
                  discard(addField.addLong(value))
              case Value.ValueNumeric(value) =>
                if (value.signum() != 0)
                  discard(addField.addNumeric(value))
              case Value.ValueText(value) =>
                if (value.nonEmpty)
                  discard(addField.addString(value))
              case Value.ValueTimestamp(value) =>
                if (value.micros != 0)
                  discard(addField.addLong(value.micros))
              case Value.ValueDate(value) =>
                if (value.days != 0)
                  discard(addField.addInt(value.days))
              case Value.ValueParty(value) =>
                // No default value for party
                discard(addField.addString(value))
              case Value.ValueBool(value) =>
                if (value)
                  discard(addField.addByte(1.toByte, s"true (bool)"))
              case Value.ValueUnit =>
              // We never write unit
            }
          case Value.ValueRecord(_, fields) =>
            // No default value for records
            discard(addField.addRecord(fields))
          case Value.ValueVariant(_, variant, value) =>
            // No default value for variant
            discard(addField.addVariant(variant, value))
          case Value.ValueContractId(value) =>
            // No default value for contractId
            discard(addField.addCid(value))
          case Value.ValueList(values) =>
            if (values.nonEmpty)
              discard(addField.addList(values))
          case Value.ValueOptional(value) =>
            value match {
              case Some(value) =>
                discard(addField.addTypedValue(value))
              case None =>
            }
          case Value.ValueTextMap(value) =>
            if (!value.isEmpty)
              discard(addTextMap(value))
          case Value.ValueGenMap(entries) =>
            if (entries.nonEmpty)
              discard(addGenMap(entries))
        }
      }
      // This delimits the end of the record.
      // Note it does not collide with the first byte of field numbers as those are always positives.
      addByte(0xff.toByte, "record end")
    }

  }

  // The purpose of a hash serves to avoid hash collisions due to equal encodings for different objects.
  // Each purpose should be used at most once.
  private[crypto] class Purpose private (val id: Byte)

  private[crypto] object Purpose {
    val Testing = new Purpose(1)
    val ContractKey = new Purpose(2)
    val MaintainerContractKeyUUID = new Purpose(4)
    val PrivateKey = new Purpose(3)
    val ContractInstance = new Purpose(5)
    val ChangeId = new Purpose(6)
    val TransactionHash = new Purpose(7)
    val MetadataHash = new Purpose(8)
  }

  private class Version private (val id: Byte)

  private object Version {
    val Legacy = new Version(0) // LF 1.x to LF 2.1
    val UpgradeFriendly = new Version(1) // from LF 2.1
  }

  class NodeHashVersion(val id: Byte)

  object NodeHashVersion {
    val V1 = new NodeHashVersion(1)
  }

  // package private for testing purpose.
  // Do not call this method from outside Hash object/
  private[crypto] def builder(
      purpose: Purpose,
      cid2Bytes: Value.ContractId => Bytes,
      upgradeFriendly: Boolean,
      numeric2Bytes: data.Numeric => Bytes = bigIntNumericToBytes,
      hashTracer: HashTracer = HashTracer.NoOp,
  ): ValueHashBuilder = {
    if (upgradeFriendly)
      new UpgradeFriendlyBuilder(purpose, cid2Bytes, numeric2Bytes, hashTracer).addVersion
    else
      new LegacyBuilder(purpose, cid2Bytes, numeric2Bytes, hashTracer).addVersion
  }

  private[crypto] def hMacBuilder(key: Hash): Builder = new HashMacBuilder(key)

  def fromHexString(s: Ref.HexString): Either[String, Hash] = {
    val bytes = Ref.HexString.decode(s)
    Either.cond(
      bytes.length == underlyingHashLength,
      new Hash(bytes),
      s"Cannot parse hash $s",
    )
  }

  def fromString(s: String): Either[String, Hash] =
    for {
      hexaString <- Ref.HexString.fromString(s)
      hash <- fromHexString(hexaString)
    } yield hash

  def assertFromString(s: String): Hash =
    data.assertRight(fromString(s))

  def hashPrivateKey(s: String): Hash =
    builder(Purpose.PrivateKey, noCid2String, upgradeFriendly = true).addString(s).build

  // This function assumes that key is well typed, i.e. :
  // 1 - `templateId` is the identifier for a template with a key of type τ
  // 2 - `key` is a value of type τ
  @throws[HashingError]
  def assertHashContractKey(
      templateId: Ref.Identifier,
      packageName: Ref.PackageName,
      key: Value,
  ): Hash = {
    val hashBuilder = builder(Purpose.ContractKey, noCid2String, upgradeFriendly = true)
    hashBuilder
      .addQualifiedName(templateId.qualifiedName)
      .addString(packageName)
      .addTypedValue(key)
      .build
  }

  def hashContractKey(
      templateId: Ref.Identifier,
      packageName: Ref.PackageName,
      key: Value,
  ): Either[HashingError, Hash] =
    handleError(assertHashContractKey(templateId, packageName: Ref.PackageName, key))

  // This function assumes that `arg` is well typed, i.e. :
  // 1 - `packageName` is the package name defined in the metadata of the package containing template `templateId`
  // 2 - `templateId` is the identifier for a template with a contract argument of type τ
  // 3 - `arg` is a value of type τ
  // The hash is not stable under suffixing of contract IDs
  @throws[HashingError]
  def assertHashContractInstance(
      templateId: Ref.Identifier,
      arg: Value,
      packageName: Ref.PackageName,
      upgradeFriendly: Boolean = false,
  ): Hash = {
    builder(Purpose.ContractInstance, aCid2Bytes, upgradeFriendly)
      .addString(packageName)
      .addIdentifier(templateId)
      .addTypedValue(arg)
      .build
  }

  def hashContractInstance(
      packageName: Ref.PackageName,
      templateId: Ref.Identifier,
      arg: Value,
  ): Either[HashingError, Hash] =
    handleError(assertHashContractInstance(templateId, arg, packageName, upgradeFriendly = true))

  def hashChangeId(
      userId: Ref.UserId,
      commandId: Ref.CommandId,
      actAs: Set[Ref.Party],
  ): Hash =
    builder(Purpose.ChangeId, noCid2String, upgradeFriendly = true)
      .addString(userId)
      .addString(commandId)
      .addStringSet(actAs)
      .build

  def deriveTransactionSeed(
      submissionSeed: Hash,
      participantId: Ref.ParticipantId,
      submitTime: Time.Timestamp,
  ): Hash =
    hMacBuilder(submissionSeed)
      .addString(participantId)
      .addLong(submitTime.micros)
      .build

  def deriveNodeSeed(
      parentDiscriminator: Hash,
      childIdx: Int,
  ): Hash =
    hMacBuilder(parentDiscriminator).addInt(childIdx).build

  def deriveContractDiscriminator(
      nodeSeed: Hash,
      submitTime: Time.Timestamp,
      parties: Set[Ref.Party],
  ): Hash =
    hMacBuilder(nodeSeed)
      .addLong(submitTime.micros)
      .addStringSet(parties)
      .build

  // For Daml-on-Corda to ensure the hashing is performed in a way that will work with upgrades.
  def deriveMaintainerContractKeyUUID(
      keyHash: Hash,
      maintainer: Ref.Party,
  ): Hash =
    builder(Purpose.MaintainerContractKeyUUID, noCid2String, upgradeFriendly = true)
      .addHash(keyHash, "Key Hash")
      .addString(maintainer)
      .build
}
