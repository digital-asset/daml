// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import cats.data.EitherT
import cats.implicits.catsSyntaxValidatedId
import cats.syntax.alternative.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.SignatureCheckError.{
  SignatureWithWrongKey,
  SignerHasNoValidKeys,
}
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.{ExecutionContext, Future}

/** Defines the default methods for protocol signing and verification that use a topology snapshot
  * for key lookup. This approach uses the signing APIs registered in Canton's
  * [[com.digitalasset.canton.crypto.Crypto]] object at node startup.
  */
class SyncCryptoSignerWithLongTermKeys(
    member: Member,
    signPublicApi: SynchronizerCryptoPureApi,
    signPrivateApi: SigningPrivateOps,
    cryptoPrivateStore: CryptoPrivateStore,
    verificationParallelismLimit: PositiveInt,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncCryptoSigner
    with NamedLogging {

  private def findSigningKey(
      topologySnapshot: TopologySnapshot,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Fingerprint] =
    for {
      signingKeys <- EitherT.right(topologySnapshot.signingKeys(member, usage))
      existingKeys <- signingKeys.toList
        .parFilterA(pk => cryptoPrivateStore.existsSigningKey(pk.fingerprint))
        .leftMap[SyncCryptoError](SyncCryptoError.StoreError.apply)
      kk <- PublicKey
        .getLatestKey(existingKeys)
        .toRight[SyncCryptoError](
          SyncCryptoError
            .KeyNotAvailable(
              member,
              KeyPurpose.Signing,
              topologySnapshot.timestamp,
              signingKeys.map(_.fingerprint),
            )
        )
        .toEitherT[FutureUnlessShutdown]
    } yield kk.fingerprint

  private def loadSigningKeysForMember(
      member: Member,
      topologySnapshot: TopologySnapshot,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Fingerprint, SigningPublicKey]] =
    topologySnapshot
      .signingKeys(member, usage)
      .map(_.map(key => (key.fingerprint, key)).toMap)

  private def loadSigningKeysForMembers(
      members: Seq[Member],
      topologySnapshot: TopologySnapshot,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, Map[Fingerprint, SigningPublicKey]]] =
    // we fetch signing keys for all members
    topologySnapshot
      .signingKeys(members, usage)
      .map(membersToKeys =>
        members
          .map(member =>
            member -> membersToKeys
              .getOrElse(member, Seq.empty)
              .map(key => (key.fingerprint, key))
              .toMap
          )
          .toMap
      )

  /** Sign given hash with signing key for (member, domain, timestamp)
    */
  override def sign(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] =
    for {
      fingerprint <- findSigningKey(topologySnapshot, usage)
      signature <- signPrivateApi
        .sign(hash, fingerprint, usage)
        .leftMap[SyncCryptoError](SyncCryptoError.SyncCryptoSigningError.apply)
    } yield signature

  private def verifySignature(
      hash: Hash,
      validKeys: Map[Fingerprint, SigningPublicKey],
      signature: Signature,
      signerStr: String,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    EitherT(Future(for {
      _ <- Either.cond(
        validKeys.nonEmpty,
        (),
        SignerHasNoValidKeys(
          s"There are no valid keys for $signerStr but received message signed with ${signature.signedBy}"
        ),
      )
      keyToUse <- validKeys
        .get(signature.signedBy)
        .toRight(
          SignatureWithWrongKey(
            s"Key ${signature.signedBy} used to generate signature is not a valid key for $signerStr. " +
              s"Valid keys are ${validKeys.values.map(_.fingerprint.unwrap)}"
          )
        )
      _ <- signPublicApi.verifySignature(hash, keyToUse, signature, usage)
    } yield ())).mapK(FutureUnlessShutdown.outcomeK)

  override def verifySignature(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    for {
      validKeys <- EitherT.right[SignatureCheckError](
        loadSigningKeysForMember(signer, topologySnapshot, usage)
      )
      _ <- verifySignature(hash, validKeys, signature, signer.toString, usage)
    } yield ()

  override def verifySignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    for {
      validKeys <- EitherT.right[SignatureCheckError](
        loadSigningKeysForMember(signer, topologySnapshot, usage)
      )
      _ <- MonadUtil.parTraverseWithLimit_(verificationParallelismLimit)(signatures.forgetNE)(
        signature => verifySignature(hash, validKeys, signature, signer.toString, usage)
      )
    } yield ()

  override def verifyGroupSignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signers: Seq[Member],
      threshold: PositiveInt,
      groupName: String,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    for {
      memberToValidKeys <- EitherT.right[SignatureCheckError](
        loadSigningKeysForMembers(signers, topologySnapshot, usage)
      )
      validKeys = memberToValidKeys.values.flatMap(_.toSeq).toMap
      keyMember = memberToValidKeys.flatMap { case (member, keyMap) =>
        keyMap.keys.map(_ -> member)
      }
      validated <- EitherT.right(
        MonadUtil.parTraverseWithLimit(verificationParallelismLimit)(signatures.forgetNE)(
          signature =>
            verifySignature(
              hash,
              validKeys,
              signature,
              groupName,
              usage,
            ).fold(
              _.invalid,
              _ => keyMember(signature.signedBy).valid[SignatureCheckError],
            )
        )
      )
      _ <- {
        val (signatureCheckErrors, validSigners) = validated.separate
        EitherT.cond[FutureUnlessShutdown](
          validSigners.distinct.sizeIs >= threshold.value, {
            if (signatureCheckErrors.nonEmpty) {
              val errors = SignatureCheckError.MultipleErrors(signatureCheckErrors)
              // TODO(i13206): Replace with an Alarm
              logger.warn(
                s"Signature check passed for $groupName, although there were errors: $errors"
              )
            }
            ()
          },
          SignatureCheckError.MultipleErrors(
            signatureCheckErrors,
            Some(s"$groupName signature threshold not reached"),
          ): SignatureCheckError,
        )
      }
    } yield ()

}
