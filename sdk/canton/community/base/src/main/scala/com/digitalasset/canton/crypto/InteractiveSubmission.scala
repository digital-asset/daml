// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.canton.protocol.hash.{
  HashTracer,
  TransactionHash,
  TransactionMetadataHashBuilder,
}
import com.digitalasset.canton.protocol.{LfContractId, LfHash, SerializableContract}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, NodeId, VersionedTransaction}
import com.digitalasset.daml.lf.value.Value.ContractId

import java.util.UUID
import scala.collection.immutable.{SortedMap, SortedSet}
import scala.concurrent.ExecutionContext

object InteractiveSubmission {
  implicit val contractIdOrdering: Ordering[LfContractId] = Ordering.by(_.coid)

  sealed trait HashError {
    def message: String
  }
  final case class HashingFailed(message: String) extends HashError
  final case class UnsupportedHashingSchemeVersion(
      version: HashingSchemeVersion,
      currentProtocolVersion: ProtocolVersion,
      minProtocolVersion: Option[ProtocolVersion],
      supportedSchemesOnCurrentPV: Set[HashingSchemeVersion],
  ) extends HashError {
    override def message: String =
      s"Hashing scheme version $version is not supported on protocol version $currentProtocolVersion." +
        s" Minimum protocol version for hashing version $version: ${minProtocolVersion.map(_.toString).getOrElse("Unsupported")}." +
        s" Supported hashing version on protocol version $currentProtocolVersion: ${supportedSchemesOnCurrentPV
            .mkString(", ")}"
  }

  object TransactionMetadataForHashing {
    def create(
        actAs: Set[Ref.Party],
        commandId: Ref.CommandId,
        transactionUUID: UUID,
        mediatorGroup: Int,
        synchronizerId: SynchronizerId,
        ledgerEffectiveTime: Option[Time.Timestamp],
        submissionTime: Time.Timestamp,
        disclosedContracts: Map[ContractId, FatContractInstance],
    ) = new TransactionMetadataForHashing(
      actAs = SortedSet.from(actAs),
      commandId = commandId,
      transactionUUID = transactionUUID,
      mediatorGroup = mediatorGroup,
      synchronizerId = synchronizerId,
      ledgerEffectiveTime = ledgerEffectiveTime,
      submissionTime = submissionTime,
      disclosedContracts = SortedMap.from(disclosedContracts),
    )

    def saltFromSerializedContract(serializedNode: SerializableContract): Bytes =
      // Salt is not hashed in V1, so it's not relevant for now, but the hashing function takes a FatContractInstance
      // so we extract it and pass it in still
      serializedNode.contractSalt
        .map(_.toProtoV30.salt)
        .map(Bytes.fromByteString)
        .getOrElse(Bytes.Empty)

    def apply(
        actAs: Set[Ref.Party],
        commandId: Ref.CommandId,
        transactionUUID: UUID,
        mediatorGroup: Int,
        synchronizerId: SynchronizerId,
        ledgerEffectiveTime: Option[Time.Timestamp],
        submissionTime: Time.Timestamp,
        disclosedContracts: Map[ContractId, SerializableContract],
    ): TransactionMetadataForHashing = {

      val asFatContracts = disclosedContracts
        .map { case (contractId, serializedNode) =>
          contractId -> FatContractInstance.fromCreateNode(
            serializedNode.toLf,
            serializedNode.ledgerCreateTime.toLf,
            saltFromSerializedContract(serializedNode),
          )
        }

      new TransactionMetadataForHashing(
        SortedSet.from(actAs),
        commandId,
        transactionUUID,
        mediatorGroup,
        synchronizerId,
        ledgerEffectiveTime,
        submissionTime,
        SortedMap.from(asFatContracts),
      )
    }
  }

  final case class TransactionMetadataForHashing private (
      actAs: SortedSet[Ref.Party],
      commandId: Ref.CommandId,
      transactionUUID: UUID,
      mediatorGroup: Int,
      synchronizerId: SynchronizerId,
      ledgerEffectiveTime: Option[Time.Timestamp],
      submissionTime: Time.Timestamp,
      disclosedContracts: SortedMap[ContractId, FatContractInstance],
  )

  private def computeHashV1(
      transaction: VersionedTransaction,
      metadata: TransactionMetadataForHashing,
      nodeSeeds: Map[NodeId, LfHash],
      hashTracer: HashTracer,
  ): Either[HashError, Hash] = {
    def catchHashingErrors[T](f: => T): Either[HashError, T] =
      scala.util
        .Try(f)
        .toEither
        .leftMap {
          case nodeHashErr: NodeHashingError => nodeHashErr.msg
          case err => err.getMessage
        }
        .leftMap(HashingFailed.apply)

    val v1Metadata = TransactionMetadataHashBuilder.MetadataV1(
      metadata.actAs,
      metadata.commandId,
      metadata.transactionUUID,
      metadata.mediatorGroup,
      metadata.synchronizerId.toProtoPrimitive,
      metadata.ledgerEffectiveTime,
      metadata.submissionTime,
      metadata.disclosedContracts,
    )

    catchHashingErrors(
      TransactionHash.tryHashTransactionWithMetadataV1(
        transaction,
        nodeSeeds,
        v1Metadata,
        hashTracer,
      )
    )
  }

  def computeVersionedHash(
      hashVersion: HashingSchemeVersion,
      transaction: VersionedTransaction,
      metadata: TransactionMetadataForHashing,
      nodeSeeds: Map[NodeId, LfHash],
      protocolVersion: ProtocolVersion,
      hashTracer: HashTracer,
  ): Either[HashError, Hash] = {
    val supportedVersions =
      HashingSchemeVersion.getHashingSchemeVersionsForProtocolVersion(protocolVersion)
    if (!supportedVersions.contains(hashVersion)) {
      Left(
        UnsupportedHashingSchemeVersion(
          hashVersion,
          protocolVersion,
          HashingSchemeVersion.minProtocolVersionForHSV(hashVersion),
          supportedVersions,
        )
      )
    } else {
      hashVersion match {
        case HashingSchemeVersion.V1 => computeHashV1(transaction, metadata, nodeSeeds, hashTracer)
      }
    }
  }

  /** Verify that the signatures provided cover the actAs parties, and are valid.
    * @param hash hash of the transaction
    * @param signatures signatures provided in the request
    * @param cryptoSnapshot topology snapshot to use to validate signatures
    * @param actAs actAs parties that should be covered by the signatures
    */
  def verifySignatures(
      hash: Hash,
      signatures: Map[PartyId, Seq[Signature]],
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
      actAs: Set[LfPartyId],
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    // ActAs parties which do not have a signature
    val actAsPartiesWithoutSignature = actAs.diff(signatures.keySet.map(_.toLf))

    if (actAsPartiesWithoutSignature.nonEmpty) {
      // We mandate that if this is an external submission, all actAs parties must have provided a signature
      EitherT.leftT[FutureUnlessShutdown, Unit](
        s"The following actAs parties did not provide an external signature: ${actAsPartiesWithoutSignature
            .mkString(", ")}"
      )
    } else {
      // Signatures coming from non act as parties. This is odd but doesn't warrant a rejection, so we just log it
      val nonActAsPartiesWithSignatures = signatures.keySet.map(_.toLf).diff(actAs)
      if (nonActAsPartiesWithSignatures.nonEmpty) {
        logger.info(
          s"The following non actAs parties provided an external signature: ${nonActAsPartiesWithSignatures
              .mkString(", ")}. Those signatures will be discarded."
        )
      }

      // We only check validity of signatures coming from the actAs parties
      val signaturesFromActAsParties =
        signatures.view
          .filterKeys(party => actAs.contains(party.toLf))
          .toMap

      // Now we do verify that all actAs signatures are valid
      verifySignatures(
        hash,
        signaturesFromActAsParties,
        cryptoSnapshot,
        logger,
      )
    }
  }

  /** Verifies that there are enough _valid_ signatures for each party to reach the threshold configured for that party.
    */
  private def verifySignatures(
      hash: Hash,
      signatures: Map[PartyId, Seq[Signature]],
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    signatures.toList
      .parTraverse_ { case (party, signatures) =>
        for {
          authInfo <- EitherT(
            cryptoSnapshot.ipsSnapshot
              .partyAuthorization(party)
              .map(
                _.toRight(s"Could not find party signing keys for $party.")
              )
          )

          (invalidSignatures, validSignatures) = signatures.map { signature =>
            authInfo.signingKeys
              .find(_.fingerprint == signature.signedBy)
              .toRight(s"Signing key ${signature.signedBy} is not a valid key for $party")
              .flatMap(key =>
                // TODO(#23551) Add new usage for interactive submission
                cryptoSnapshot.pureCrypto
                  .verifySignature(hash.unwrap, key, signature, SigningKeyUsage.ProtocolOnly)
                  .map(_ => key.fingerprint)
                  .leftMap(_.toString)
              )
          }.separate
          validSignaturesSet = validSignatures.toSet
          _ = {
            // Log invalid signatures at info level because it is unexpected,
            // but doesn't mandate that we fail validation as long as there are still threshold-many valid signatures
            // as asserted just below
            invalidSignatures.foreach { invalidSignature =>
              logger.info(s"Invalid signature for $party: $invalidSignature")
            }
          }
          _ <- EitherT.cond[FutureUnlessShutdown](
            validSignaturesSet.sizeIs >= authInfo.threshold.unwrap,
            (),
            s"Received ${validSignatures.size} valid signatures (${invalidSignatures.size} invalid), but expected at least ${authInfo.threshold} valid for $party",
          )
        } yield {
          logger.debug(
            s"Found ${validSignaturesSet.size} valid external signatures for $party with threshold ${authInfo.threshold.unwrap}"
          )
        }
      }
}
