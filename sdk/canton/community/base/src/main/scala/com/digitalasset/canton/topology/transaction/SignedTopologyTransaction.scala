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
import com.digitalasset.canton.data.CantonTimestamp
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
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class SignedTopologyTransaction[+Op <: TopologyChangeOp, +M <: TopologyMapping](
    transaction: TopologyTransaction[Op, M],
    signatures: NonEmpty[Set[Signature]],
    isProposal: Boolean,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SignedTopologyTransaction.type
    ]
) extends HasProtocolVersionedWrapper[
      SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]
    ]
    with DelegatedTopologyTransactionLike[Op, M]
    with Product
    with Serializable
    with PrettyPrinting {

  override protected def transactionLikeDelegate: TopologyTransactionLike[Op, M] = transaction

  lazy val hashOfSignatures: Hash = Hash.digest(
    HashPurpose.TopologyTransactionSignature,
    signatures.toList
      .sortBy(_.signedBy.toProtoPrimitive)
      .map(_.toProtoV30.toByteString)
      .reduceLeft(_.concat(_)),
    HashAlgorithm.Sha256,
  )

  def addSignatures(add: Seq[Signature]): SignedTopologyTransaction[Op, M] =
    SignedTopologyTransaction(
      transaction,
      signatures ++ add,
      isProposal,
    )(representativeProtocolVersion)

  def removeSignatures(keys: Set[Fingerprint]): Option[SignedTopologyTransaction[Op, M]] =
    NonEmpty
      .from(signatures.filterNot(sig => keys.contains(sig.signedBy)))
      .map(updatedSignatures => copy(signatures = updatedSignatures))

  @transient override protected lazy val companionObj: SignedTopologyTransaction.type =
    SignedTopologyTransaction

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectMapping[TargetMapping <: TopologyMapping: ClassTag]
      : Option[SignedTopologyTransaction[Op, TargetMapping]] = {
    transaction
      .selectMapping[TargetMapping]
      .map(_ => this.asInstanceOf[SignedTopologyTransaction[Op, TargetMapping]])
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectOp[TargetOp <: TopologyChangeOp: ClassTag]
      : Option[SignedTopologyTransaction[TargetOp, M]] =
    transaction
      .selectOp[TargetOp]
      .map(_ => this.asInstanceOf[SignedTopologyTransaction[TargetOp, M]])

  def select[TargetOp <: TopologyChangeOp: ClassTag, TargetMapping <: TopologyMapping: ClassTag]
      : Option[SignedTopologyTransaction[TargetOp, TargetMapping]] = {
    selectMapping[TargetMapping].flatMap(_.selectOp[TargetOp])
  }

  def toProtoV30: v30.SignedTopologyTransaction =
    v30.SignedTopologyTransaction(
      transaction = transaction.getCryptographicEvidence,
      signatures = signatures.toSeq.map(_.toProtoV30),
      proposal = isProposal,
    )

  override def pretty: Pretty[SignedTopologyTransaction.this.type] =
    prettyOfClass(
      unnamedParam(_.transaction),
      param("signatures", _.signatures.map(_.signedBy)),
      paramIfTrue("proposal", _.isProposal),
    )

  def restrictedToDomain: Option[DomainId] = transaction.mapping.restrictedToDomain

  @VisibleForTesting
  def copy[Op2 <: TopologyChangeOp, M2 <: TopologyMapping](
      transaction: TopologyTransaction[Op2, M2] = this.transaction,
      signatures: NonEmpty[Set[Signature]] = this.signatures,
      isProposal: Boolean = this.isProposal,
  ): SignedTopologyTransaction[Op2, M2] =
    new SignedTopologyTransaction[Op2, M2](transaction, signatures, isProposal)(
      representativeProtocolVersion
    )
}

object SignedTopologyTransaction
    extends HasProtocolVersionedWithOptionalValidationCompanion[
      SignedTopologyTransaction[TopologyChangeOp, TopologyMapping],
    ] {

  val InitialTopologySequencingTime: CantonTimestamp = CantonTimestamp.MinValue.immediateSuccessor

  override val name: String = "SignedTopologyTransaction"

  type GenericSignedTopologyTransaction =
    SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]

  type PositiveSignedTopologyTransaction =
    SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(
      v30.SignedTopologyTransaction
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  import com.digitalasset.canton.resource.DbStorage.Implicits.*

  /** Sign the given topology transaction. */
  def create[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: TopologyTransaction[Op, M],
      signingKeys: NonEmpty[Set[Fingerprint]],
      isProposal: Boolean,
      crypto: CryptoPrivateApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, SigningError, SignedTopologyTransaction[Op, M]] =
    for {
      signaturesNE <- signingKeys.toSeq.toNEF.parTraverse(
        crypto.sign(transaction.hash.hash, _)
      )
      representativeProtocolVersion = supportedProtoVersions.protocolVersionRepresentativeFor(
        protocolVersion
      )
    } yield SignedTopologyTransaction(transaction, signaturesNE.toSet, isProposal)(
      representativeProtocolVersion
    )

  def asVersion[Op <: TopologyChangeOp, M <: TopologyMapping](
      signedTx: SignedTopologyTransaction[Op, M],
      protocolVersion: ProtocolVersion,
  )(
      crypto: Crypto
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, String, SignedTopologyTransaction[Op, M]] = {
    val originTx = signedTx.transaction

    // Convert and resign the transaction if the topology transaction version does not match the expected version
    if (!originTx.isEquivalentTo(protocolVersion)) {
      if (signedTx.signatures.size > 1) {
        EitherT.leftT(
          s"Failed to resign topology transaction $originTx with multiple signatures, as only one signature is supported"
        )
      } else {
        val convertedTx = originTx.asVersion(protocolVersion)
        SignedTopologyTransaction
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
      transactionP: v30.SignedTopologyTransaction,
  ): ParsingResult[GenericSignedTopologyTransaction] = {
    val v30.SignedTopologyTransaction(txBytes, signaturesP, isProposal) = transactionP
    for {
      transaction <- TopologyTransaction.fromByteString(protocolVersionValidation)(txBytes)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "SignedTopologyTransactionX.signatures",
        signaturesP,
      )
      rpv <- supportedProtoVersions.protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SignedTopologyTransaction(transaction, signatures.toSet, isProposal)(rpv)
  }

  def createGetResultDomainTopologyTransaction: GetResult[GenericSignedTopologyTransaction] =
    GetResult { r =>
      fromTrustedByteString(r.<<[ByteString]).valueOr(err =>
        throw new DbSerializationException(
          s"Failed to deserialize SignedTopologyTransactionX: $err"
        )
      )
    }

  implicit def setParameterTopologyTransaction(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[GenericSignedTopologyTransaction] = {
    (d: GenericSignedTopologyTransaction, pp: PositionedParameters) =>
      pp >> d.toByteArray
  }
}
