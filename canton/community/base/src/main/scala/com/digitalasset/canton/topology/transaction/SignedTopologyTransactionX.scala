// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.data.EitherT
import cats.instances.seq.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbSerializationException
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/** A signed topology transaction
  *
  * Every topology transaction needs to be authorized by an appropriate key. This object represents such
  * an authorization, where there is a signature of a given key of the given topology transaction.
  *
  * Whether the key is eligible to authorize the topology transaction depends on the topology state
  */
final case class SignedTopologyTransactionX[+Op <: TopologyChangeOpX, +M <: TopologyMappingX](
    transaction: TopologyTransactionX[Op, M],
    signatures: NonEmpty[Set[Signature]],
    isProposal: Boolean,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SignedTopologyTransactionX.type
    ]
) extends HasProtocolVersionedWrapper[
      SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]
    ]
    with Product
    with Serializable
    with PrettyPrinting {

  lazy val hashOfSignatures: Hash = Hash.digest(
    HashPurpose.TopologyTransactionSignature,
    signatures.toList
      .sortBy(_.signedBy.toProtoPrimitive)
      .map(_.toProtoV30.toByteString)
      .reduceLeft(_.concat(_)),
    HashAlgorithm.Sha256,
  )

  def addSignatures(add: Seq[Signature]): SignedTopologyTransactionX[Op, M] =
    SignedTopologyTransactionX(
      transaction,
      signatures ++ add,
      isProposal,
    )(representativeProtocolVersion)

  def operation: Op = transaction.op

  def mapping: M = transaction.mapping

  @transient override protected lazy val companionObj: SignedTopologyTransactionX.type =
    SignedTopologyTransactionX

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectMapping[TargetMapping <: TopologyMappingX: ClassTag]
      : Option[SignedTopologyTransactionX[Op, TargetMapping]] = {
    transaction
      .selectMapping[TargetMapping]
      .map(_ => this.asInstanceOf[SignedTopologyTransactionX[Op, TargetMapping]])
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectOp[TargetOp <: TopologyChangeOpX: ClassTag]
      : Option[SignedTopologyTransactionX[TargetOp, M]] =
    transaction
      .selectOp[TargetOp]
      .map(_ => this.asInstanceOf[SignedTopologyTransactionX[TargetOp, M]])

  def select[TargetOp <: TopologyChangeOpX: ClassTag, TargetMapping <: TopologyMappingX: ClassTag]
      : Option[SignedTopologyTransactionX[TargetOp, TargetMapping]] = {
    selectMapping[TargetMapping].flatMap(_.selectOp[TargetOp])
  }

  def toProtoV30: v30.SignedTopologyTransactionX =
    v30.SignedTopologyTransactionX(
      transaction = transaction.getCryptographicEvidence,
      signatures = signatures.toSeq.map(_.toProtoV30),
      proposal = isProposal,
    )

  override def pretty: Pretty[SignedTopologyTransactionX.this.type] =
    prettyOfClass(
      unnamedParam(_.transaction),
      param("signatures", _.signatures.map(_.signedBy)),
      paramIfTrue("proposal", _.isProposal),
    )

  def restrictedToDomain: Option[DomainId] = transaction.mapping.restrictedToDomain

  @VisibleForTesting
  def copy[Op2 <: TopologyChangeOpX, M2 <: TopologyMappingX](
      transaction: TopologyTransactionX[Op2, M2] = this.transaction,
      signatures: NonEmpty[Set[Signature]] = this.signatures,
      isProposal: Boolean = this.isProposal,
  ): SignedTopologyTransactionX[Op2, M2] =
    new SignedTopologyTransactionX[Op2, M2](transaction, signatures, isProposal)(
      representativeProtocolVersion
    )
}

object SignedTopologyTransactionX
    extends HasProtocolVersionedWithOptionalValidationCompanion[
      SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX],
    ] {
  override val name: String = "SignedTopologyTransactionX"

  type GenericSignedTopologyTransactionX =
    SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]

  type PositiveSignedTopologyTransactionX =
    SignedTopologyTransactionX[TopologyChangeOpX.Replace, TopologyMappingX]

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v30.SignedTopologyTransactionX
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  import com.digitalasset.canton.resource.DbStorage.Implicits.*

  /** Sign the given topology transaction. */
  def create[Op <: TopologyChangeOpX, M <: TopologyMappingX](
      transaction: TopologyTransactionX[Op, M],
      signingKeys: NonEmpty[Set[Fingerprint]],
      isProposal: Boolean,
      crypto: CryptoPrivateApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, SigningError, SignedTopologyTransactionX[Op, M]] =
    for {
      signaturesNE <- signingKeys.toSeq.toNEF.parTraverse(
        crypto.sign(transaction.hash.hash, _)
      )
      representativeProtocolVersion = supportedProtoVersions.protocolVersionRepresentativeFor(
        protocolVersion
      )
    } yield SignedTopologyTransactionX(transaction, signaturesNE.toSet, isProposal)(
      representativeProtocolVersion
    )

  def asVersion[Op <: TopologyChangeOpX, M <: TopologyMappingX](
      signedTx: SignedTopologyTransactionX[Op, M],
      protocolVersion: ProtocolVersion,
  )(
      crypto: Crypto
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, String, SignedTopologyTransactionX[Op, M]] = {
    val originTx = signedTx.transaction

    // Convert and resign the transaction if the topology transaction version does not match the expected version
    if (!originTx.isEquivalentTo(protocolVersion)) {
      if (signedTx.signatures.size > 1) {
        EitherT.leftT(
          s"Failed to resign topology transaction $originTx with multiple signatures, as only one signature is supported"
        )
      } else {
        val convertedTx = originTx.asVersion(protocolVersion)
        SignedTopologyTransactionX
          .create(
            convertedTx,
            signedTx.signatures.map(_.signedBy),
            signedTx.isProposal,
            crypto.privateCrypto,
            protocolVersion,
          )
          .leftMap { err =>
            s"Failed to resign topology transaction $originTx (${originTx.representativeProtocolVersion}) for domain version $protocolVersion: $err"
          }
      }
    } else
      EitherT.rightT(signedTx)
  }

  def fromProtoV30(
      protocolVersionValidation: ProtocolVersionValidation,
      transactionP: v30.SignedTopologyTransactionX,
  ): ParsingResult[GenericSignedTopologyTransactionX] = {
    val v30.SignedTopologyTransactionX(txBytes, signaturesP, isProposal) = transactionP
    for {
      transaction <- TopologyTransactionX.fromByteString(protocolVersionValidation)(txBytes)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "SignedTopologyTransactionX.signatures",
        signaturesP,
      )
    } yield SignedTopologyTransactionX(transaction, signatures.toSet, isProposal)(
      supportedProtoVersions.protocolVersionRepresentativeFor(ProtoVersion(2))
    )

  }

  def createGetResultDomainTopologyTransaction: GetResult[GenericSignedTopologyTransactionX] =
    GetResult { r =>
      fromByteStringUnsafe(r.<<[ByteString]).valueOr(err =>
        throw new DbSerializationException(
          s"Failed to deserialize SignedTopologyTransactionX: $err"
        )
      )
    }

  implicit def setParameterTopologyTransaction(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[GenericSignedTopologyTransactionX] = {
    (d: GenericSignedTopologyTransactionX, pp: PositionedParameters) =>
      pp >> d.toByteArray
  }
}
