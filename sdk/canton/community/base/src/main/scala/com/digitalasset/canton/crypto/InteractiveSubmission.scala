// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.implicits.{catsSyntaxParallelTraverse1, toBifunctorOps, toTraverseOps}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.canton.{CommandId, LfPartyId}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

object InteractiveSubmission {
  sealed trait HashError

  // TODO(i20660): We hash the command ID only for now while the proper hashing algorithm is being designed
  def computeHashV1(commandId: CommandId): Hash =
    Hash
      .digest(
        HashPurpose.PreparedSubmission,
        ByteString.copyFromUtf8(commandId.unwrap),
        HashAlgorithm.Sha256,
      )

  def computeVersionedHash(
      hashVersion: HashingSchemeVersion,
      commandId: CommandId,
  ): Either[HashError, Hash] =
    hashVersion match {
      case HashingSchemeVersion.V1 => Right(computeHashV1(commandId))
    }

  def verifySignatures(
      hash: Hash,
      signatures: Map[PartyId, Seq[Signature]],
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Set[LfPartyId]] =
    signatures.toList
      .parTraverse { case (party, signatures) =>
        for {
          authInfo <- EitherT(
            cryptoSnapshot.ipsSnapshot
              .partyAuthorization(party)
              .map(
                _.toRight(s"Could not find party signing keys for $party.")
              )
          )

          validSignatures <- EitherT.fromEither[FutureUnlessShutdown](signatures.traverse {
            signature =>
              authInfo.signingKeys
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
            validSignaturesSet.sizeIs == validSignatures.size,
            (),
            s"The following signatures were provided one or more times for $party, all signatures must be unique: ${validSignatures
                .diff(validSignaturesSet.toList)}",
          )
          _ <- EitherT.cond[FutureUnlessShutdown](
            validSignaturesSet.sizeIs >= authInfo.threshold.unwrap,
            (),
            s"Received ${validSignatures.size} signatures, but expected ${authInfo.threshold} for $party",
          )
        } yield party.toLf
      }
      .map(_.toSet)

}
