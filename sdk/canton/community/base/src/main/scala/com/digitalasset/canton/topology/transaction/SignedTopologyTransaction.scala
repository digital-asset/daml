// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.data.EitherT
import cats.instances.order.*
import cats.instances.seq.*
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbSerializationException
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{SynchronizerId, TopologyManager}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.ExecutionContext
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

  def hashOfSignatures(protocolVersion: ProtocolVersion): Hash = {
    val builder = Hash.build(HashPurpose.TopologyTransactionSignature, HashAlgorithm.Sha256)
    signatures.toList
      .sortBy(_.signedBy.toProtoPrimitive)
      .foreach(tx => builder.add(tx.toByteString(protocolVersion)))
    builder.finish()
  }

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
      : Option[SignedTopologyTransaction[Op, TargetMapping]] =
    transaction
      .selectMapping[TargetMapping]
      .map(_ => this.asInstanceOf[SignedTopologyTransaction[Op, TargetMapping]])

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectOp[TargetOp <: TopologyChangeOp: ClassTag]
      : Option[SignedTopologyTransaction[TargetOp, M]] =
    transaction
      .selectOp[TargetOp]
      .map(_ => this.asInstanceOf[SignedTopologyTransaction[TargetOp, M]])

  def select[TargetOp <: TopologyChangeOp: ClassTag, TargetMapping <: TopologyMapping: ClassTag]
      : Option[SignedTopologyTransaction[TargetOp, TargetMapping]] =
    selectMapping[TargetMapping].flatMap(_.selectOp[TargetOp])

  def toProtoV30: v30.SignedTopologyTransaction =
    v30.SignedTopologyTransaction(
      transaction = transaction.getCryptographicEvidence,
      signatures = signatures.toSeq.map(_.toProtoV30),
      proposal = isProposal,
    )

  override protected def pretty: Pretty[SignedTopologyTransaction.this.type] =
    prettyOfClass(
      unnamedParam(_.transaction),
      // just calling `signatures.map(_.signedBy)` hides the fact that there could be
      // multiple (possibly invalid) signatures by the same key
      param("signatures", _.signatures.toSeq.map(_.signedBy).sorted),
      paramIfTrue("proposal", _.isProposal),
    )

  def restrictedToSynchronizer: Option[SynchronizerId] =
    transaction.mapping.restrictedToSynchronizer

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
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v33)(
      v30.SignedTopologyTransaction
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  import com.digitalasset.canton.resource.DbStorage.Implicits.*

  /** Sign the given topology transaction. */
  def create[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: TopologyTransaction[Op, M],
      signingKeys: NonEmpty[Map[Fingerprint, NonEmpty[Set[SigningKeyUsage]]]],
      isProposal: Boolean,
      crypto: CryptoPrivateApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, SigningError, SignedTopologyTransaction[Op, M]] =
    for {
      signaturesNE <- signingKeys.toSeq.toNEF.parTraverse { case (keyId, usage) =>
        crypto.sign(transaction.hash.hash, keyId, usage)
      }
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
  ): EitherT[FutureUnlessShutdown, String, SignedTopologyTransaction[Op, M]] = {
    val originTx = signedTx.transaction

    // Convert and resign the transaction if the topology transaction version does not match the expected version
    if (!originTx.isEquivalentTo(protocolVersion)) {
      if (signedTx.signatures.sizeIs > 1) {
        EitherT.leftT(
          s"Failed to resign topology transaction $originTx with multiple signatures, as only one signature is supported"
        )
      } else {
        val convertedTx = originTx.asVersion(protocolVersion)
        val keysWithUsage = TopologyManager
          .assignExpectedUsageToKeys(
            convertedTx.mapping,
            signedTx.signatures.map(signature => signature.signedBy),
          )
        for {
          signedTopologyTransaction <- SignedTopologyTransaction
            .create(
              convertedTx,
              keysWithUsage,
              signedTx.isProposal,
              crypto.privateCrypto,
              protocolVersion,
            )
            .leftMap { err =>
              s"Failed to resign topology transaction $originTx (${originTx.representativeProtocolVersion}) for " +
                s"synchronizer version $protocolVersion: $err"
            }
        } yield signedTopologyTransaction
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
        "SignedTopologyTransaction.signatures",
        signaturesP,
      )
      rpv <- supportedProtoVersions.protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SignedTopologyTransaction(transaction, signatures.toSet, isProposal)(rpv)
  }

  def createGetResultSynchronizerTopologyTransaction: GetResult[GenericSignedTopologyTransaction] =
    GetResult { r =>
      fromTrustedByteString(r.<<[ByteString]).valueOr(err =>
        throw new DbSerializationException(
          s"Failed to deserialize SignedTopologyTransaction: $err"
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

final case class SignedTopologyTransactions[
    +Op <: TopologyChangeOp,
    +M <: TopologyMapping,
](transactions: Seq[SignedTopologyTransaction[Op, M]])(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SignedTopologyTransactions.type
    ]
) extends HasProtocolVersionedWrapper[SignedTopologyTransactions[TopologyChangeOp, TopologyMapping]]
    with PrettyPrinting {

  @transient override protected lazy val companionObj: SignedTopologyTransactions.type =
    SignedTopologyTransactions

  override protected def pretty: Pretty[SignedTopologyTransactions.this.type] = prettyOfParam(
    _.transactions
  )

  def toProtoV30: v30.SignedTopologyTransactions = v30.SignedTopologyTransactions(
    transactions.map(_.toByteString)
  )

  def collectOfType[T <: TopologyChangeOp: ClassTag]: SignedTopologyTransactions[T, M] =
    SignedTopologyTransactions(
      transactions.mapFilter(_.selectOp[T])
    )(representativeProtocolVersion)

  def collectOfMapping[T <: TopologyMapping: ClassTag]: SignedTopologyTransactions[Op, T] =
    SignedTopologyTransactions(
      transactions.mapFilter(_.selectMapping[T])
    )(representativeProtocolVersion)
}

object SignedTopologyTransactions
    extends HasProtocolVersionedWithContextCompanion[
      SignedTopologyTransactions[TopologyChangeOp, TopologyMapping],
      ProtocolVersion,
    ] {
  override val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v33)(
      v30.SignedTopologyTransactions
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  override def name: String = "SignedTopologyTransactions"

  type PositiveSignedTopologyTransactions =
    Seq[SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]]

  def apply[Op <: TopologyChangeOp, M <: TopologyMapping](
      transactions: Seq[SignedTopologyTransaction[Op, M]],
      protocolVersion: ProtocolVersion,
  ): SignedTopologyTransactions[Op, M] =
    SignedTopologyTransactions(transactions)(
      supportedProtoVersions.protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      proto: v30.SignedTopologyTransactions,
  ): ParsingResult[SignedTopologyTransactions[TopologyChangeOp, TopologyMapping]] =
    for {
      transactions <- proto.signedTransaction
        .traverse(
          SignedTopologyTransaction.fromByteString(expectedProtocolVersion)(
            ProtocolVersionValidation.PV(expectedProtocolVersion)
          )(_)
        )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SignedTopologyTransactions(transactions)(rpv)

  /** Merges the signatures of transactions with the same transaction hash,
    * while maintaining the order of the first occurrence of each hash.
    *
    * For example:
    * {{{
    * val original = Seq(hash_A, hash_B, hash_A, hash_C, hash_B)
    * compact(original) == Seq(hash_A, hash_B, hash_C)
    * }}}
    */
  def compact(
      txs: Seq[GenericSignedTopologyTransaction]
  ): Seq[GenericSignedTopologyTransaction] = {
    val byHash = txs
      .groupBy(_.hash)
      .view
      .mapValues(_.reduceLeftOption((tx1, tx2) => tx1.addSignatures(tx2.signatures.toSeq)))
      .collect { case (k, Some(v)) => k -> v }
      .toMap

    val (compacted, _) =
      txs.foldLeft((Vector.empty[GenericSignedTopologyTransaction], byHash)) {
        case ((result, byHash), tx) =>
          val newResult = byHash.get(tx.hash).map(result :+ _).getOrElse(result)
          val txHashRemoved = byHash.removed(tx.hash)
          (newResult, txHashRemoved)
      }
    compacted
  }

  def collectOfMapping[Op <: TopologyChangeOp, M <: TopologyMapping: ClassTag](
      transactions: Seq[SignedTopologyTransaction[Op, TopologyMapping]]
  ): Seq[SignedTopologyTransaction[Op, M]] =
    transactions.mapFilter(_.selectMapping[M])
}
