// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.data.EitherT
import cats.instances.order.*
import cats.instances.seq.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbSerializationException
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.TopologyManager.assignExpectedUsageToKeys
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

/** A signed topology transaction
  *
  * Every topology transaction needs to be authorized by an appropriate key. This object represents
  * such an authorization, where there is a signature of a given key of the given topology
  * transaction.
  *
  * Whether the key is eligible to authorize the topology transaction depends on the topology state.
  *
  * Invariant: All `signatures` have a different fingerprint.
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class SignedTopologyTransaction[+Op <: TopologyChangeOp, +M <: TopologyMapping] private (
    transaction: TopologyTransaction[Op, M],
    // All signatures from both the single and multi transaction hashes
    // May or may not cover the transaction hash.
    signatures: NonEmpty[Set[TopologyTransactionSignature]],
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
  {
    val duplicateSigningKeys = signatures.toSeq
      .map(_.authorizingLongTermKey)
      .groupBy1(identity)
      .filter(_._2.sizeIs > 1)
      .keySet
    require(duplicateSigningKeys.isEmpty, s"Duplicate signing keys used: $duplicateSigningKeys")
  }

  def allUnvalidatedSignaturesCoveringHash: Set[TopologyTransactionSignature] =
    signatures.filter(_.coversHash(transaction.hash))

  private val singleHashSignatures: Set[Signature] =
    signatures.collect { case SingleTransactionSignature(_, signature) =>
      signature
    }

  /** Computes a set of hash -> signature pairs for all signatures
    */
  def signaturesWithHash(
      hashOps: HashOps
  ): Set[(Hash, TopologyTransactionSignature)] =
    signatures
      .map { signature =>
        signature.computeHashForSignatureVerification(hashOps) -> signature
      }

  override protected def transactionLikeDelegate: TopologyTransactionLike[Op, M] = transaction

  def hashOfSignatures(protocolVersion: ProtocolVersion): Hash = {
    val builder = Hash.build(HashPurpose.TopologyTransactionSignature, HashAlgorithm.Sha256)
    signatures.toList
      .sortBy(_.authorizingLongTermKey.toProtoPrimitive)
      .foreach(signature =>
        builder.addByteString(signature.signature.toByteString(protocolVersion))
      )
    builder.finish()
  }

  /** Add new signatures into the existing ones. Important: this method DOES NOT check that the
    * added signatures are consistent with this transaction, and specifically does not check that
    * multi-transaction signatures cover this transaction hash. New signatures from signing keys,
    * which have already signed the transaction, are discarded.
    */
  def addSignatures(
      newSignatures: NonEmpty[Set[TopologyTransactionSignature]]
  ): SignedTopologyTransaction[Op, M] = {
    val signingKeysOfExistingSignatures = signatures.map(_.authorizingLongTermKey)
    SignedTopologyTransaction(
      transaction,
      signatures ++ newSignatures.filter(newSig =>
        !signingKeysOfExistingSignatures.contains(newSig.authorizingLongTermKey)
      ),
      isProposal,
    )(representativeProtocolVersion)
  }

  /** Add new signature into the existing ones. Important: this method DOES NOT check that the added
    * signatures are consistent with this transaction, and specifically does not check that
    * multi-transaction signatures cover this transaction hash. New signatures from signing keys,
    * which have already signed the transaction, are discarded.
    */
  def addSingleSignature(
      newSignature: Signature
  ): SignedTopologyTransaction[Op, M] = addSingleSignatures(NonEmpty.mk(Set, newSignature))

  /** Add new signatures into the existing ones. Important: this method DOES NOT check that the
    * added signatures are consistent with this transaction, and specifically does not check that
    * multi-transaction signatures cover this transaction hash.
    */
  def addSingleSignatures(
      newSignatures: NonEmpty[Set[Signature]]
  ): SignedTopologyTransaction[Op, M] =
    addSignatures(newSignatures.map(SingleTransactionSignature(transaction.hash, _)))

  def removeSignatures(keys: Set[Fingerprint]): Option[SignedTopologyTransaction[Op, M]] = {
    val updatedSignatures =
      signatures.filterNot(sig => keys.contains(sig.authorizingLongTermKey))

    NonEmpty
      .from(updatedSignatures)
      .map(updatedSignaturesNE => copy(signatures = updatedSignaturesNE))
  }

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

  def toProtoV30: v30.SignedTopologyTransaction = {
    // Collect the multi transaction signatures and group them by multi-hash to avoid creating multiple
    // proto objects for signatures covering the same multi-hash
    val multiHashSignatures = signatures.collect { case sig: MultiTransactionSignature => sig }
    val multiTransactionSignatures = {
      multiHashSignatures.groupBy(_.transactionHashes).view.map { case (hashes, signatures) =>
        v30.MultiTransactionSignatures(
          transactionHashes = hashes.map(_.hash.getCryptographicEvidence).toSeq,
          signatures = signatures.toSeq.map(_.signature.toProtoV30),
        )
      }
    }.toSeq

    v30.SignedTopologyTransaction(
      transaction = transaction.getCryptographicEvidence,
      signatures = singleHashSignatures.map(_.toProtoV30).toSeq,
      proposal = isProposal,
      multiTransactionSignatures = multiTransactionSignatures,
    )
  }

  override protected def pretty: Pretty[SignedTopologyTransaction.this.type] =
    prettyOfClass(
      unnamedParam(_.transaction),
      // just calling `signatures.map(_.signedBy)` hides the fact that there could be
      // multiple (possibly invalid) signatures by the same key
      param("signatures", _.signatures.toSeq.map(_.authorizingLongTermKey).sorted),
      paramIfTrue("proposal", _.isProposal),
    )

  def restrictedToSynchronizer: Option[SynchronizerId] =
    transaction.mapping.restrictedToSynchronizer

  @VisibleForTesting
  def copy[Op2 <: TopologyChangeOp, M2 <: TopologyMapping](
      transaction: TopologyTransaction[Op2, M2] = this.transaction,
      signatures: NonEmpty[Set[TopologyTransactionSignature]] = this.signatures,
      isProposal: Boolean = this.isProposal,
  ): SignedTopologyTransaction[Op2, M2] =
    new SignedTopologyTransaction[Op2, M2](transaction, signatures, isProposal)(
      representativeProtocolVersion
    )

  def updateIsProposal(isProposal: Boolean) =
    new SignedTopologyTransaction(this.transaction, this.signatures, isProposal)(
      representativeProtocolVersion
    )
}

object SignedTopologyTransaction
    extends VersioningCompanionContext[
      SignedTopologyTransaction[TopologyChangeOp, TopologyMapping],
      // Validation is done in synchronizer store but not in authorized store
      ProtocolVersionValidation,
    ] {

  val InitialTopologySequencingTime: CantonTimestamp = CantonTimestamp.MinValue.immediateSuccessor

  override val name: String = "SignedTopologyTransaction"

  type GenericSignedTopologyTransaction =
    SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]

  type PositiveSignedTopologyTransaction =
    SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(
      v30.SignedTopologyTransaction
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def withTopologySignatures[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: TopologyTransaction[Op, M],
      signatures: NonEmpty[Seq[TopologyTransactionSignature]],
      isProposal: Boolean,
      protocolVersion: ProtocolVersion,
  ): SignedTopologyTransaction[Op, M] = SignedTopologyTransaction(
    transaction = transaction,
    signatures = TopologyTransactionSignature.distinctSignatures(signatures),
    isProposal = isProposal,
  )(protocolVersionRepresentativeFor(protocolVersion))

  def withSignature[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: TopologyTransaction[Op, M],
      signature: Signature,
      isProposal: Boolean,
      protocolVersion: ProtocolVersion,
  ): SignedTopologyTransaction[Op, M] =
    withSignatures(
      transaction,
      NonEmpty.mk(Seq, signature),
      isProposal = isProposal,
      protocolVersion,
    )

  def withSignatures[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: TopologyTransaction[Op, M],
      signatures: NonEmpty[Seq[Signature]],
      isProposal: Boolean,
      protocolVersion: ProtocolVersion,
  ): SignedTopologyTransaction[Op, M] =
    SignedTopologyTransaction(
      transaction = transaction,
      signatures = TopologyTransactionSignature.distinctSignatures(
        signatures.map(SingleTransactionSignature(transaction.hash, _))
      ),
      isProposal = isProposal,
    )(protocolVersionRepresentativeFor(protocolVersion))

  def duplicateSigningKeys(
      signatures: NonEmpty[Set[TopologyTransactionSignature]]
  ): Set[Fingerprint] = signatures.toSeq
    .map(_.authorizingLongTermKey)
    .groupBy1(identity)
    .filter(_._2.sizeIs > 1)
    .keySet

  def create[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: TopologyTransaction[Op, M],
      signatures: NonEmpty[Set[TopologyTransactionSignature]],
      isProposal: Boolean,
      protocolVersion: ProtocolVersion,
  ): Either[String, SignedTopologyTransaction[Op, M]] = {
    val duplicates = duplicateSigningKeys(signatures)
    Either.cond(
      duplicates.isEmpty,
      SignedTopologyTransaction[Op, M](
        transaction,
        signatures,
        isProposal,
      )(protocolVersionRepresentativeFor(protocolVersion)),
      s"Transaction has duplicate signatures: ${duplicates.mkString(", ")}",
    )
  }

  private def signAndCreateWithAssignedKeyUsages[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: TopologyTransaction[Op, M],
      keysWithUsage: NonEmpty[Map[Fingerprint, NonEmpty[Set[SigningKeyUsage]]]],
      isProposal: Boolean,
      crypto: CryptoPrivateApi,
      protocolVersion: ProtocolVersion,
      multiTxAndHashOps: Option[(NonEmpty[Set[TxHash]], HashOps)],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, SigningError, SignedTopologyTransaction[Op, M]] =
    for {
      hash <- EitherT.fromEither[FutureUnlessShutdown](
        multiTxAndHashOps
          .traverse { case (hashes, ops) =>
            Either.cond(
              hashes.contains(transaction.hash),
              MultiTransactionSignature.computeCombinedHash(hashes, ops),
              SigningError.InvariantViolation(
                s"Multi hash set ${hashes.map(_.hash.toHexString).mkString(", ")} does not contain transaction hash ${transaction.hash.hash.toHexString}"
              ),
            )
          }
          .map(_.getOrElse(transaction.hash.hash))
      )
      signaturesNE <- keysWithUsage.toSeq.toNEF.parTraverse { case (keyId, usage) =>
        crypto.sign(hash, keyId, usage)
      }
      representativeProtocolVersion = versioningTable.protocolVersionRepresentativeFor(
        protocolVersion
      )
      topologyTransactionSignatures = multiTxAndHashOps
        .map { case (hashes, _) =>
          signaturesNE.map[TopologyTransactionSignature](MultiTransactionSignature(hashes, _))
        }
        .getOrElse(
          signaturesNE
            .map[TopologyTransactionSignature](SingleTransactionSignature(transaction.hash, _))
        )
        .toSet
    } yield SignedTopologyTransaction(transaction, topologyTransactionSignatures, isProposal)(
      representativeProtocolVersion
    )

  @VisibleForTesting
  def signAndCreateWithAssignedKeyUsages[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: TopologyTransaction[Op, M],
      keysWithUsage: NonEmpty[Map[Fingerprint, NonEmpty[Set[SigningKeyUsage]]]],
      isProposal: Boolean,
      crypto: CryptoPrivateApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, SigningError, SignedTopologyTransaction[Op, M]] =
    signAndCreateWithAssignedKeyUsages(
      transaction,
      keysWithUsage,
      isProposal,
      crypto,
      protocolVersion,
      None,
    )

  /** Sign the given topology transaction.
    * @param multiHash,
    *   if provided the multi hash will be signed instead of the transaction hash
    */
  def signAndCreate[Op <: TopologyChangeOp, M <: TopologyMapping](
      transaction: TopologyTransaction[Op, M],
      signingKeys: NonEmpty[Set[Fingerprint]],
      isProposal: Boolean,
      crypto: CryptoPrivateApi,
      protocolVersion: ProtocolVersion,
      multiHash: Option[(NonEmpty[Set[TxHash]], HashOps)] = None,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, SigningError, SignedTopologyTransaction[Op, M]] = {
    val keysWithUsage = assignExpectedUsageToKeys(
      transaction.mapping,
      signingKeys,
      forSigning = true,
    )
    signAndCreateWithAssignedKeyUsages(
      transaction,
      keysWithUsage,
      isProposal,
      crypto,
      protocolVersion,
      multiHash,
    )
  }

  /** @param crypto
    *   We use a [[com.digitalasset.canton.crypto.BaseCrypto]] because this method serves both the
    *   synchronizer outbox dispatcher that requires a
    *   [[com.digitalasset.canton.crypto.SynchronizerCrypto]] and the GRPC topology manager read
    *   service that uses a [[com.digitalasset.canton.crypto.Crypto]]. This method is only used to
    *   produce signatures; and it does not verify signatures from untrusted sources.
    */
  def asVersion[Op <: TopologyChangeOp, M <: TopologyMapping](
      signedTx: SignedTopologyTransaction[Op, M],
      protocolVersion: ProtocolVersion,
  )(
      crypto: BaseCrypto
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
        for {
          signedTopologyTransaction <- SignedTopologyTransaction
            .signAndCreate(
              convertedTx,
              signedTx.signatures.map(signature => signature.authorizingLongTermKey),
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
    val v30.SignedTopologyTransaction(
      txBytes,
      signaturesP,
      isProposal,
      multiTransactionSignaturesPO,
    ) = transactionP
    for {
      transaction <- TopologyTransaction.fromByteString(protocolVersionValidation, txBytes)

      singleSignatures <- signaturesP
        .traverse(Signature.fromProtoV30)
        .map(
          _.map(SingleTransactionSignature(transaction.hash, _))
        )

      multiTransactionHashes <- multiTransactionSignaturesPO
        .flatTraverse(MultiTransactionSignature.fromProtoV30(_, transaction.hash).map(_.forgetNE))

      allSignaturesWithDuplicates <- NonEmpty
        .from(singleSignatures ++ multiTransactionHashes)
        .toRight(
          ProtoDeserializationError
            .InvariantViolation("signatures", "At least one signature must be provided")
        )

      /*
      Added as part of https://github.com/DACH-NY/canton-network-internal/issues/1063
      Some signature schemes (e.g., EC-DSA) are non-deterministic which means that one key
      can lead to arbitrary many signatures. Due to a bug in the validation, we accumulated
      many signatures for the same key, leading to high validation time and memory usage.
      As a workaround, we discard duplicate signatures.
       */
      allSignaturesWithoutDuplicates = TopologyTransactionSignature.distinctSignatures(
        allSignaturesWithDuplicates
      )

      rpv <- versioningTable.protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SignedTopologyTransaction(transaction, allSignaturesWithoutDuplicates, isProposal)(rpv)
  }

  def createGetResultSynchronizerTopologyTransaction(implicit
      getByteString: GetResult[ByteString]
  ): GetResult[GenericSignedTopologyTransaction] =
    GetResult { r =>
      fromTrustedByteStringPVV(r.<<[ByteString]).valueOr(err =>
        throw new DbSerializationException(
          s"Failed to deserialize SignedTopologyTransaction: $err"
        )
      )
    }

  implicit def setParameterTopologyTransaction(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[GenericSignedTopologyTransaction] =
    (d: GenericSignedTopologyTransaction, pp: PositionedParameters) => pp >> d.toByteArray
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
    extends VersioningCompanionContext[
      SignedTopologyTransactions[TopologyChangeOp, TopologyMapping],
      ProtocolVersionValidation,
    ] {
  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(
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
      versioningTable.protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      expectedProtocolVersion: ProtocolVersionValidation,
      proto: v30.SignedTopologyTransactions,
  ): ParsingResult[SignedTopologyTransactions[TopologyChangeOp, TopologyMapping]] =
    for {
      transactions <- proto.signedTransaction
        .traverse(
          SignedTopologyTransaction.fromByteString(
            expectedProtocolVersion,
            expectedProtocolVersion,
            _,
          )
        )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SignedTopologyTransactions(transactions)(rpv)

  /** Merges the signatures of transactions with the same transaction hash, while maintaining the
    * order of the first occurrence of each hash.
    *
    * For example:
    * {{{
    * val original = Seq(hash_A, hash_B, hash_A, hash_C, hash_B)
    * compact(original) == Seq(hash_A, hash_B, hash_C)
    * }}}
    */
  def compact(
      txs: Seq[GenericSignedTopologyTransaction]
  ): Seq[GenericSignedTopologyTransaction] =
    txs.zipWithIndex
      .groupBy1 { case (tx, _) => tx.hash }
      .values
      .toSeq
      .view
      .map(
        _.reduceLeft[(GenericSignedTopologyTransaction, Int)] {
          case ((tx1, index1), (tx2, index2)) =>
            (tx1.addSignatures(tx2.signatures), index1.min(index2))
        }
      )
      .sortBy { case (_, index) => index }
      .map { case (tx, _) => tx }
      .toSeq

  def collectOfMapping[Op <: TopologyChangeOp, M <: TopologyMapping: ClassTag](
      transactions: Seq[SignedTopologyTransaction[Op, TopologyMapping]]
  ): Seq[SignedTopologyTransaction[Op, M]] =
    transactions.mapFilter(_.selectMapping[M])
}
