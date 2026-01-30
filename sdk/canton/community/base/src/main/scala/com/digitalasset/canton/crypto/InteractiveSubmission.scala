// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.LedgerTimeBoundaries
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.canton.protocol.hash.{HashTracer, TransactionHash}
import com.digitalasset.canton.protocol.{LfContractId, LfHash}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, NodeId, VersionedTransaction}
import com.digitalasset.daml.lf.value.Value.ContractId

import java.util.UUID
import scala.collection.immutable.{SortedMap, SortedSet}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

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
        timeBoundaries: LedgerTimeBoundaries,
        preparationTime: Time.Timestamp,
        maxRecordTime: Option[Time.Timestamp],
        disclosedContracts: Map[ContractId, FatContractInstance],
    ) = new TransactionMetadataForHashing(
      actAs = SortedSet.from(actAs),
      commandId = commandId,
      transactionUUID = transactionUUID,
      mediatorGroup = mediatorGroup,
      synchronizerId = synchronizerId,
      timeBoundaries = timeBoundaries,
      preparationTime = preparationTime,
      maxRecordTime = maxRecordTime,
      disclosedContracts = SortedMap.from(disclosedContracts),
    )
  }

  final case class TransactionMetadataForHashing private (
      actAs: SortedSet[Ref.Party],
      commandId: Ref.CommandId,
      transactionUUID: UUID,
      mediatorGroup: Int,
      synchronizerId: SynchronizerId,
      timeBoundaries: LedgerTimeBoundaries,
      preparationTime: Time.Timestamp,
      maxRecordTime: Option[Time.Timestamp],
      disclosedContracts: SortedMap[ContractId, FatContractInstance],
  )

  private def tryComputeHash(
      hashVersion: HashingSchemeVersion,
      transaction: VersionedTransaction,
      metadata: TransactionMetadataForHashing,
      nodeSeeds: Map[NodeId, LfHash],
      hashTracer: HashTracer,
  ): Hash =
    TransactionHash.tryHashTransactionWithMetadata(
      hashVersion,
      transaction,
      nodeSeeds,
      metadata,
      hashTracer,
    )

  private def catchHashingErrors(f: => Hash): Either[HashError, Hash] =
    Try(f) match {
      case Success(value) => Right(value)
      case Failure(err) =>
        Left(HashingFailed(err match {
          case nodeHashErr: NodeHashingError => nodeHashErr.msg
          case err => err.getMessage
        }))
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
      catchHashingErrors(
        tryComputeHash(hashVersion, transaction, metadata, nodeSeeds, hashTracer)
      )
    }
  }

  /** Verify that the signatures provided cover the actAs parties, and are valid.
    * @param hash
    *   hash of the transaction
    * @param signatures
    *   signatures provided in the request
    * @param cryptoPureApi
    *   crypto pure api to use to verify signatures
    * @param topologySnapshot
    *   topology snapshot to use to validate signatures
    * @param actAs
    *   actAs parties that should be covered by the signatures
    */
  def verifySignatures(
      hash: Hash,
      signatures: Map[PartyId, Seq[Signature]],
      cryptoPureApi: CryptoPureApi,
      topologySnapshot: TopologySnapshot,
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
        cryptoPureApi,
        topologySnapshot,
        logger,
      )
    }
  }

  /** Verifies that there are enough _valid_ signatures for each party to reach the threshold
    * configured for that party.
    */
  private def verifySignatures(
      hash: Hash,
      signatures: Map[PartyId, Seq[Signature]],
      cryptoPureApi: CryptoPureApi,
      topologySnapshot: TopologySnapshot,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    signatures.toList
      .parTraverse_ { case (party, signatures) =>
        for {
          signingKeysWithThreshold <- EitherT(
            topologySnapshot
              .signingKeysWithThreshold(party)
              .map(
                _.toRight(s"Could not find party signing keys for $party.")
              )
          )

          (invalidSignatures, validSignatures) = signatures.map { signature =>
            signingKeysWithThreshold.keys
              .find(_.fingerprint == signature.authorizingLongTermKey)
              .toRight(
                s"Signing key ${signature.authorizingLongTermKey} is not a valid key for $party"
              )
              .flatMap(key =>
                cryptoPureApi
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
            validSignaturesSet.sizeIs >= signingKeysWithThreshold.threshold.unwrap,
            (),
            s"Received ${validSignaturesSet.size} valid signatures from distinct keys (${invalidSignatures.size} invalid), but expected at least ${signingKeysWithThreshold.threshold} valid for $party. " +
              s"Transaction hash to be signed: ${hash.toHexString}. Ensure the correct transaction hash is signed with the correct key(s).",
          )
        } yield {
          logger.debug(
            s"Found ${validSignaturesSet.size} valid external signatures for $party with threshold ${signingKeysWithThreshold.threshold.unwrap}"
          )
        }
      }
}
