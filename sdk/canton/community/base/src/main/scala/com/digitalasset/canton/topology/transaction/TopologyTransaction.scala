// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.ProtoDeserializationError.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.PrettyInstances.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.transaction
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString
import slick.jdbc.SetParameter

import scala.reflect.ClassTag

/** Replace or Remove */
sealed trait TopologyChangeOp extends Product with Serializable with PrettyPrinting {
  def toProto: v30.Enums.TopologyChangeOp

  final def select[TargetOp <: TopologyChangeOp](implicit
      O: ClassTag[TargetOp]
  ): Option[TargetOp] = O.unapply(this)

  override def pretty: Pretty[TopologyChangeOp.this.type] = adHocPrettyInstance
}

object TopologyChangeOp {

  /** Adds or replaces an existing record */
  final case object Replace extends TopologyChangeOp {
    override def toProto: v30.Enums.TopologyChangeOp =
      v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE
  }
  final case object Remove extends TopologyChangeOp {
    override def toProto: v30.Enums.TopologyChangeOp =
      v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_REMOVE
  }

  type Remove = Remove.type
  type Replace = Replace.type

  def unapply(
      tx: TopologyTransaction[TopologyChangeOp, TopologyMapping]
  ): Option[TopologyChangeOp] = Some(tx.operation)
  def unapply(
      tx: SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]
  ): Option[TopologyChangeOp] = Some(tx.operation)

  def fromProtoV30(
      protoOp: v30.Enums.TopologyChangeOp
  ): ParsingResult[Option[TopologyChangeOp]] =
    protoOp match {
      case v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_UNSPECIFIED => Right(None)
      case v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_REMOVE => Right(Some(Remove))
      case v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE => Right(Some(Replace))
      case v30.Enums.TopologyChangeOp.Unrecognized(x) => Left(UnrecognizedEnum(protoOp.name, x))
    }

  implicit val setParameterTopologyChangeOp: SetParameter[TopologyChangeOp] = (v, pp) =>
    v match {
      case Remove => pp.setInt(1)
      case Replace => pp.setInt(2)
    }

}

trait TopologyTransactionLike[+Op <: TopologyChangeOp, +M <: TopologyMapping] {
  def operation: Op
  def serial: PositiveInt
  def mapping: M
  def hash: TxHash
}

trait DelegatedTopologyTransactionLike[+Op <: TopologyChangeOp, +M <: TopologyMapping]
    extends TopologyTransactionLike[Op, M] {
  protected def transactionLikeDelegate: TopologyTransactionLike[Op, M]
  override final def operation: Op = transactionLikeDelegate.operation
  override final def serial: PositiveInt = transactionLikeDelegate.serial
  override final def mapping: M = transactionLikeDelegate.mapping
  override final def hash: TxHash = transactionLikeDelegate.hash
}

/** Change to the distributed domain topology
  *
  * A topology transaction is a state change to the domain topology. There are different
  * types of topology states (so called mappings, because they map some id to some value).
  *
  * Each mapping has some variables and some combination of these variables makes a
  * "unique key". Subsequent changes to that key need to have an incremental serial number.
  *
  * Topology changes always affect certain identities. Therefore, these topology
  * transactions need to be authorized through signatures.
  *
  * An authorized transaction is called a [[SignedTopologyTransaction]]
  */
final case class TopologyTransaction[+Op <: TopologyChangeOp, +M <: TopologyMapping] private (
    operation: Op,
    serial: PositiveInt,
    mapping: M,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TopologyTransaction.type
    ],
    override val deserializedFrom: Option[ByteString] = None,
) extends TopologyTransactionLike[Op, M]
    with ProtocolVersionedMemoizedEvidence
    with PrettyPrinting
    with HasProtocolVersionedWrapper[TopologyTransaction[TopologyChangeOp, TopologyMapping]] {

  def reverse: TopologyTransaction[TopologyChangeOp, M] = {
    val next = (operation: TopologyChangeOp) match {
      case TopologyChangeOp.Replace => TopologyChangeOp.Remove
      case TopologyChangeOp.Remove => TopologyChangeOp.Replace
    }
    TopologyTransaction(next, serial = serial.increment, mapping = mapping)(
      representativeProtocolVersion,
      None,
    )
  }
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectMapping[TargetMapping <: TopologyMapping: ClassTag]
      : Option[TopologyTransaction[Op, TargetMapping]] =
    mapping
      .select[TargetMapping]
      .map(_ => this.asInstanceOf[TopologyTransaction[Op, TargetMapping]])

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectOp[TargetOp <: TopologyChangeOp: ClassTag]: Option[TopologyTransaction[TargetOp, M]] =
    operation.select[TargetOp].map(_ => this.asInstanceOf[TopologyTransaction[TargetOp, M]])

  /** returns hash of the given transaction */
  lazy val hash: TxHash = {
    TxHash(
      Hash.digest(
        HashPurpose.TopologyTransactionSignature,
        // TODO(#14048) use digest directly to avoid protobuf serialization for hashing
        this.getCryptographicEvidence,
        HashAlgorithm.Sha256,
      )
    )
  }

  override def toByteStringUnmemoized: ByteString = super[HasProtocolVersionedWrapper].toByteString

  def toProtoV30: v30.TopologyTransaction = v30.TopologyTransaction(
    operation = operation.toProto,
    serial = serial.value,
    mapping = Some(mapping.toProtoV30),
  )

  def asVersion(
      protocolVersion: ProtocolVersion
  ): TopologyTransaction[Op, M] = {
    TopologyTransaction[Op, M](operation, serial, mapping)(
      TopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)
    )
  }

  /** Indicates how to pretty print this instance.
    * See `PrettyPrintingTest` for examples on how to implement this method.
    */
  override def pretty: Pretty[TopologyTransaction.this.type] =
    prettyOfClass(
      unnamedParam(_.mapping),
      param("serial", _.serial),
      param("operation", _.operation),
    )

  @transient override protected lazy val companionObj: TopologyTransaction.type =
    TopologyTransaction
}

object TopologyTransaction
    extends HasMemoizedProtocolVersionedWrapperCompanion[
      TopologyTransaction[TopologyChangeOp, TopologyMapping]
    ] {

  final case class TxHash(hash: Hash) extends AnyVal {}

  override val name: String = "TopologyTransaction"

  type GenericTopologyTransaction = TopologyTransaction[TopologyChangeOp, TopologyMapping]

  val supportedProtoVersions: transaction.TopologyTransaction.SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.TopologyTransaction)(
        supportedProtoVersionMemoized(_)(fromProtoV30),
        _.toProtoV30.toByteString,
      )
    )

  def apply[Op <: TopologyChangeOp, M <: TopologyMapping](
      op: Op,
      serial: PositiveInt,
      mapping: M,
      protocolVersion: ProtocolVersion,
  ): TopologyTransaction[Op, M] = TopologyTransaction[Op, M](op, serial, mapping)(
    protocolVersionRepresentativeFor(protocolVersion),
    None,
  )

  private def fromProtoV30(transactionP: v30.TopologyTransaction)(
      bytes: ByteString
  ): ParsingResult[TopologyTransaction[TopologyChangeOp, TopologyMapping]] = {
    val v30.TopologyTransaction(opP, serialP, mappingP) = transactionP
    for {
      mapping <- ProtoConverter.parseRequired(TopologyMapping.fromProtoV30, "mapping", mappingP)
      serial <- ProtoConverter.parsePositiveInt("serial", serialP)
      op <- ProtoConverter.parseEnum(TopologyChangeOp.fromProtoV30, "operation", opP)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield TopologyTransaction(op, serial, mapping)(
      rpv,
      Some(bytes),
    )
  }
}
