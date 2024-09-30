// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.data.EitherT
import cats.syntax.apply.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, Hash, Signature}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.*

import scala.concurrent.ExecutionContext

// For now only used to propagate the signatures up to the ConfirmationRequestFactory to be verified there
// Later on will also be part of the transaction view and be shipped to confirming participants so they can
// also validate the signatures
/** Signatures provided by non-hosted parties
  */
final case class TransactionAuthorizationPartySignatures private (
    signatures: Map[PartyId, Seq[Signature]]
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TransactionAuthorizationPartySignatures.type
    ]
) extends HasProtocolVersionedWrapper[TransactionAuthorizationPartySignatures]
    with PrettyPrinting {
  @transient override protected lazy val companionObj
      : TransactionAuthorizationPartySignatures.type =
    TransactionAuthorizationPartySignatures

  private[canton] def toProtoV30: v30.TransactionAuthorizationPartySignatures =
    v30.TransactionAuthorizationPartySignatures(
      signatures = signatures.map { case (party, signatures) =>
        v30.TransactionAuthorizationSinglePartySignatures(
          party.toProtoPrimitive,
          signatures.map(_.toProtoV30),
        )
      }.toSeq
    )

  override def pretty: Pretty[this.type] = prettyOfClass(
    param("signatures", _.signatures)
  )

  def copy(
      signatures: Map[PartyId, Seq[Signature]]
  ): TransactionAuthorizationPartySignatures =
    TransactionAuthorizationPartySignatures(signatures)(representativeProtocolVersion)

  def verifySignatures(hash: Hash, cryptoSnapshot: DomainSnapshotSyncCryptoApi)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Set[LfPartyId]] =
    signatures.toList
      .parTraverse { case (party, signatures) =>
        for {
          validKeys <- EitherT(
            cryptoSnapshot.ipsSnapshot
              .partyAuthorization(party)
              .map(
                _.toRight(s"Could not find party signing keys for $party.")
              )
          )
          validSignatures <- EitherT.fromEither[FutureUnlessShutdown](signatures.traverse {
            signature =>
              validKeys.signingKeys
                .find(_.fingerprint == signature.signedBy)
                .toRight(s"Signing key ${signature.signedBy} is not a valid key for $party")
                .flatMap(key =>
                  cryptoSnapshot.pureCrypto
                    .verifySignature(hash, key, signature)
                    .map(_ => key.fingerprint)
                    .leftMap(_.toString)
                )
          })
          validSignaturesSet = validSignatures.toSet
          _ <- EitherT.cond[FutureUnlessShutdown](
            validSignaturesSet.size == validSignatures.size,
            (),
            s"The following signatures were provided one or more times for $party, all signatures must be unique: ${validSignatures
                .diff(validSignaturesSet.toList)}",
          )
          _ <- EitherT.cond[FutureUnlessShutdown](
            validSignaturesSet.size >= validKeys.threshold.unwrap,
            (),
            s"Received ${validSignatures.size} signatures, but expected ${validKeys.threshold} for $party",
          )
        } yield party.toLf
      }
      .map(_.toSet)
}

object TransactionAuthorizationPartySignatures
    extends HasProtocolVersionedCompanion[TransactionAuthorizationPartySignatures]
    with ProtocolVersionedCompanionDbHelpers[TransactionAuthorizationPartySignatures] {

  def apply(
      signatures: Map[PartyId, Seq[Signature]],
      protocolVersion: ProtocolVersion,
  ): TransactionAuthorizationPartySignatures =
    TransactionAuthorizationPartySignatures(signatures)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  override def name: String = "TransactionAuthorizationPartySignatures"

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v32)(
      v30.TransactionAuthorizationPartySignatures
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private[canton] def fromProtoV30(
      proto: v30.TransactionAuthorizationPartySignatures
  ): ParsingResult[TransactionAuthorizationPartySignatures] = {
    val v30.TransactionAuthorizationPartySignatures(signaturesP) = proto
    for {
      signatures <- signaturesP.traverse {
        case v30.TransactionAuthorizationSinglePartySignatures(party, signatures) =>
          (
            PartyId.fromProtoPrimitive(party, "party"),
            signatures.traverse(Signature.fromProtoV30),
          ).tupled
      }
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield TransactionAuthorizationPartySignatures(signatures.toMap, rpv.representative)
  }
}
