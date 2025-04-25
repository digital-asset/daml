// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.verifier

import cats.data.EitherT
import cats.implicits.{catsSyntaxAlternativeSeparate, catsSyntaxValidatedId}
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.EncryptionAlgorithmSpec.RsaOaepSha256
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.SignatureCheckError.{
  SignatureWithWrongKey,
  SignerHasNoValidKeys,
}
import com.digitalasset.canton.crypto.SigningAlgorithmSpec.Ed25519
import com.digitalasset.canton.crypto.SymmetricKeyScheme.Aes128Gcm
import com.digitalasset.canton.crypto.provider.jce.JcePureCrypto
import com.digitalasset.canton.crypto.{
  Fingerprint,
  Hash,
  PbkdfScheme,
  Signature,
  SignatureCheckError,
  SignatureDelegation,
  SigningKeyUsage,
  SigningPublicKey,
  SynchronizerCryptoPureApi,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.github.blemale.scaffeine.{Cache, Scaffeine}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, *}

/** Aggregates all methods related to protocol messages' signature verification. If a signature
  * delegation is present, verification uses the session key included in the signature after it has
  * been validated; otherwise, it defaults to the original method and verifies the signature with
  * the long-term key. These methods require a topology snapshot to ensure the correct signing keys
  * are used, based on the current state (i.e., OwnerToKeyMappings).
  *
  * @param verifyPublicApiWithLongTermKeys
  *   The crypto public API used to directly verify messages or validate a signature delegation with
  *   a long-term key.
  * @param verificationParallelismLimit
  *   The maximum number of concurrent verifications allowed.
  */
class SyncCryptoVerifier(
    synchronizerId: SynchronizerId,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    verifyPublicApiWithLongTermKeys: SynchronizerCryptoPureApi,
    verificationParallelismLimit: PositiveInt,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** The software-based crypto public API that is used to verify signatures with a session signing
    * key (generated in software). Except for the supported signing schemes, all other schemes are
    * not needed. Therefore, we use fixed schemes (i.e. placeholders) for the other crypto
    * parameters.
    *
    * //TODO(#23731): Split up pure crypto into smaller modules and only use the signing module here
    */
  private lazy val verifyPublicApiSoftwareBased: SynchronizerCryptoPureApi = {
    val pureCryptoForSessionKeys = new JcePureCrypto(
      defaultSymmetricKeyScheme = Aes128Gcm, // not used
      defaultSigningAlgorithmSpec =
        Ed25519, // not used, as this crypto interface is only for signature verification, and the scheme is derived
      // directly from the signature.
      supportedSigningAlgorithmSpecs =
        verifyPublicApiWithLongTermKeys.supportedSigningAlgorithmSpecs,
      defaultEncryptionAlgorithmSpec = RsaOaepSha256, // not used
      supportedEncryptionAlgorithmSpecs = NonEmpty.mk(Set, RsaOaepSha256), // not used
      defaultHashAlgorithm = Sha256, // not used
      defaultPbkdfScheme = PbkdfScheme.Argon2idMode1, // not used
      loggerFactory = loggerFactory,
    )

    new SynchronizerCryptoPureApi(staticSynchronizerParameters, pureCryptoForSessionKeys)
  }

  // Caches the valid signature delegations received as part of the signatures so that we don't need
  // to verify them every time.
  private[canton] val sessionKeysVerificationCache
      : Cache[Fingerprint, (SignatureDelegation, FiniteDuration)] =
    Scaffeine()
      .expireAfter[Fingerprint, (SignatureDelegation, FiniteDuration)](
        create = (_, cacheData) => {
          val (_, retentionTime) = cacheData
          retentionTime
        },
        update = (_, _, d) => d,
        read = (_, _, d) => d,
      )
      // allow the JVM garbage collector to remove entries from it when there is pressure on memory
      .softValues()
      .build()

  private def loadSigningKeysForMember(
      member: Member,
      topologySnapshot: TopologySnapshot,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Map[Fingerprint, SigningPublicKey]] =
    EitherT.right[SignatureCheckError](
      topologySnapshot
        .signingKeys(member, usage)
        .map(_.map(key => (key.fingerprint, key)).toMap)
    )

  private def loadSigningKeysForMembers(
      members: Seq[Member],
      topologySnapshot: TopologySnapshot,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Map[
    Member,
    Map[Fingerprint, SigningPublicKey],
  ]] =
    EitherT.right[SignatureCheckError](
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
    )

  private def getValidKeys(
      topologySnapshot: TopologySnapshot,
      signers: Seq[Member],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Map[Fingerprint, SigningPublicKey]] =
    signers match {
      case Seq(singleSigner) => loadSigningKeysForMember(singleSigner, topologySnapshot, usage)
      case _ =>
        loadSigningKeysForMembers(signers, topologySnapshot, usage)
          .map(_.values.flatMap(_.toSeq).toMap)
    }

  private def verifySignatureWithLongTermKey(
      validKeys: Map[Fingerprint, SigningPublicKey],
      hash: Hash,
      signature: Signature,
      signerStr: String,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    (for {
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
      _ <- verifyPublicApiWithLongTermKeys.verifySignature(hash, keyToUse, signature, usage)
    } yield ()).toEitherT[FutureUnlessShutdown]

  private def verifySignatureWithSessionKey(
      signatureDelegation: SignatureDelegation,
      topologySnapshot: TopologySnapshot,
      validKeysO: Option[Map[Fingerprint, SigningPublicKey]],
      hash: Hash,
      signature: Signature,
      signers: Seq[Member],
      signerStr: String,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = {
    val SignatureDelegation(sessionKey, validityPeriod, _) = signatureDelegation
    val currentTimestamp = topologySnapshot.timestamp

    def verifyWithValidSessionKey =
      verifyPublicApiSoftwareBased
        .verifySignature(hash, sessionKey, signature, usage)
        .toEitherT[FutureUnlessShutdown]

    def invalidSessionKey =
      EitherT.leftT[FutureUnlessShutdown, Unit](
        SignatureCheckError.InvalidSignatureDelegation(
          "The current signature delegation" +
            s"is only valid from ${validityPeriod.fromInclusive} to ${validityPeriod.toExclusive} while the " +
            s"current timestamp is $currentTimestamp"
        ): SignatureCheckError
      )

    def verifySessionKey(validKeys: Map[Fingerprint, SigningPublicKey]) =
      for {
        _ <- verifySignatureWithLongTermKey(
          validKeys,
          SignatureDelegation.generateHash(
            synchronizerId,
            signatureDelegation.sessionKey,
            signatureDelegation.validityPeriod,
          ),
          signatureDelegation.signature,
          signerStr,
          usage,
        ).leftMap[SignatureCheckError](err =>
          SignatureCheckError.InvalidSignatureDelegation(err.show)
        )
        _ <- verifyWithValidSessionKey
        dynamicSynchronizerParameters <- EitherT(
          topologySnapshot.findDynamicSynchronizerParameters()
        )
          .leftMap[SignatureCheckError](err =>
            SignatureCheckError.MissingDynamicSynchronizerParameters(err)
          )

        expirationTime = signatureDelegation.validityPeriod.toExclusive - currentTimestamp
        safetyMargin = (dynamicSynchronizerParameters.parameters.confirmationResponseTimeout +
          dynamicSynchronizerParameters.parameters.mediatorReactionTimeout).duration

        /* The safety margin should be as large as the expected delay in using old topology snapshots.
         * That's by default the sum of the dynamic synchronizer parameters confirmationResponseTimeout and
         * mediatorReactionTimeout (plus the assumed drift - 30 seconds) in the participant's processing
         * speed over sequencer speed).
         */
        retentionTimeMillis = expirationTime.toMillis + safetyMargin.toMillis + 30.seconds.toMillis

        /* The signature delegation is valid, so we can store it for future use. Since
         * the delegation signature is not added at the moment of creation, it may be stored in the cache
         * for longer than its validity period. We can accept this because, even though the key is in the cache,
         * it is invalid and cannot be used.
         */
        _ = sessionKeysVerificationCache.put(
          sessionKey.id,
          (signatureDelegation, FiniteDuration(retentionTimeMillis, MILLISECONDS)),
        )
      } yield ()

    // we store the received and validated signature delegation in a cache to avoid re-verifying it
    val cachedSignatureDelegationO =
      sessionKeysVerificationCache.getIfPresent(sessionKey.fingerprint).map { case (sD, _) => sD }
    for {
      // get the current long-term valid keys
      validKeys <- validKeysO.fold(
        getValidKeys(topologySnapshot, signers, usage)
      )(validKeys => EitherT.rightT[FutureUnlessShutdown, SignatureCheckError](validKeys))
      // delegation is in the cache, and it's valid, so we can use it to directly verify the signature
      _ <-
        if (
          cachedSignatureDelegationO.contains(signatureDelegation) &&
          signatureDelegation.isValidAt(currentTimestamp) &&
          validKeys.contains(signatureDelegation.delegatingKeyId)
        ) verifyWithValidSessionKey
        // the delegation is no longer valid for this timestamp
        else if (!signatureDelegation.isValidAt(currentTimestamp)) invalidSessionKey
        // there is no delegation in the cache so we need to validate the session key with the long-term key
        // and store the delegation for future use
        else verifySessionKey(validKeys)
    } yield ()
  }

  private def verifySignatureInternal(
      topologySnapshot: TopologySnapshot,
      validKeysO: Option[Map[Fingerprint, SigningPublicKey]],
      hash: Hash,
      signature: Signature,
      signers: Seq[Member],
      signerStr: String,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    signature.signatureDelegation match {
      case Some(signatureDelegation) =>
        verifySignatureWithSessionKey(
          signatureDelegation,
          topologySnapshot: TopologySnapshot,
          validKeysO: Option[Map[Fingerprint, SigningPublicKey]],
          hash: Hash,
          signature: Signature,
          signers: Seq[Member],
          signerStr: String,
          usage: NonEmpty[Set[SigningKeyUsage]],
        )
      // a signature with no session key delegation, so we run the verification with the long-term key
      case None =>
        for {
          validKeys <- getValidKeys(topologySnapshot, signers, usage)
          _ <- verifySignatureWithLongTermKey(
            validKeys,
            hash,
            signature,
            signerStr,
            usage,
          )
        } yield ()
    }

  /** Verify a given signature using the currently active signing keys in the current topology
    * state.
    */
  def verifySignature(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    verifySignatureInternal(
      topologySnapshot,
      None,
      hash,
      signature,
      Seq(signer),
      signer.toString,
      usage,
    )

  /** Verifies multiple signatures using the currently active signing keys in the current topology
    * state.
    */
  def verifySignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    MonadUtil.parTraverseWithLimit_(verificationParallelismLimit)(signatures.forgetNE)(signature =>
      verifySignatureInternal(
        topologySnapshot,
        None,
        hash,
        signature,
        Seq(signer),
        signer.toString,
        usage,
      )
    )

  /** Verifies multiple group signatures using the currently active signing keys of the different
    * signers in the current topology state.
    *
    * @param threshold
    *   the number of valid signatures required for the overall verification to be considered
    *   correct.
    */
  def verifyGroupSignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signers: Seq[Member],
      threshold: PositiveInt,
      groupName: String,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    for {
      validKeysWithMembers <- loadSigningKeysForMembers(signers, topologySnapshot, usage)
      keyMember = validKeysWithMembers.flatMap { case (member, keyMap) =>
        keyMap.keys.map(fingerprint => fingerprint -> member)
      }
      validKeys = validKeysWithMembers.values.flatMap(_.toSeq).toMap
      validated <- EitherT.right(
        MonadUtil.parTraverseWithLimit(verificationParallelismLimit)(signatures.forgetNE) {
          signature =>
            verifySignatureInternal(
              topologySnapshot,
              Some(validKeys),
              hash,
              signature,
              signers,
              groupName,
              usage,
            ).fold(
              _.invalid,
              _ => {
                signature.signatureDelegation match {
                  case Some(signatureDelegation) =>
                    keyMember(signatureDelegation.signature.signedBy).valid[SignatureCheckError]
                  case None => keyMember(signature.signedBy).valid[SignatureCheckError]
                }
              },
            )
        }
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

object SyncCryptoVerifier {

  def create(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      pureCrypto: SynchronizerCryptoPureApi,
      verificationParallelismLimit: PositiveInt,
      loggerFactory: NamedLoggerFactory,
  ) =
    new SyncCryptoVerifier(
      synchronizerId,
      staticSynchronizerParameters,
      pureCrypto,
      verificationParallelismLimit,
      loggerFactory,
    )

}
