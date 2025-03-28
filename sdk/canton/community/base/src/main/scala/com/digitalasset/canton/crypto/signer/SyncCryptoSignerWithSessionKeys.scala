// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.SessionSigningKeysConfig
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.EncryptionAlgorithmSpec.RsaOaepSha256
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.SymmetricKeyScheme.Aes128Gcm
import com.digitalasset.canton.crypto.provider.jce.{JcePrivateCrypto, JcePureCrypto}
import com.digitalasset.canton.crypto.signer.SyncCryptoSignerWithSessionKeys.SessionKeyAndDelegation
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** Defines the methods for protocol message signing and verification using a session signing key.
  * This requires signatures to include information about which session key is being used, as well
  * as an authorization by a long-term key through an additional signature. This extra signature
  * covers the session key, its validity period, and the synchronizer for which it is valid. This
  * allows us to use the session key, within a specific time frame and synchronizer, to sign
  * protocol messages instead of using the long-term key. If the signature delegation is not
  * present, verification defaults to the original method by attempting to verify the signature with
  * the long-term key. Session keys are intended to be used with a KMS/HSM-based provider to reduce
  * the number of signing calls and, consequently, lower the latency costs associated with such
  * external key management services.
  *
  * @param signPrivateApiDefault
  *   The crypto private API that is used to sign session signing keys and verify the corresponding
  *   signature delegation with a long-term key.
  */
class SyncCryptoSignerWithSessionKeys(
    synchronizerId: SynchronizerId,
    member: Member,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    signPrivateApiDefault: SigningPrivateOps,
    sessionSigningKeysConfig: SessionSigningKeysConfig,
    supportedSigningAlgorithmSpecs: NonEmpty[Set[SigningAlgorithmSpec]],
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncCryptoSigner {

  /** The software-based crypto public API that is used to sign protocol messages and verify their
    * signatures with a session signing key. Except for the signing algorithm and key
    * specifications, all other schemes are not needed. Therefore, we use fixed schemes (i.e.
    * placeholders) for the other crypto parameters. // TODO(#23731): Split up pure crypto into
    * smaller modules and only use the signing module here
    */
  private val signPublicApiWithSessionKeys = {
    val pureCryptoForSessionKeys = new JcePureCrypto(
      Aes128Gcm,
      sessionSigningKeysConfig.signingAlgorithmSpec,
      supportedSigningAlgorithmSpecs,
      RsaOaepSha256,
      NonEmpty.mk(Set, RsaOaepSha256),
      Sha256,
      PbkdfScheme.Argon2idMode1,
      loggerFactory,
    )

    new SynchronizerCryptoPureApi(staticSynchronizerParameters, pureCryptoForSessionKeys)
  }

  /** The user-configured validity period of a session signing key. */
  private val sessionKeyValidityPeriod =
    PositiveSeconds.fromConfig(sessionSigningKeysConfig.keyValidityDuration)

  /** The key specification for the session signing keys. */
  private val sessionKeySpec = sessionSigningKeysConfig.signingKeySpec

  /** A cut-off duration that determines when the key should stop being used to prevent signature
    * verification failures due to unpredictable sequencing timestamps.
    */
  @VisibleForTesting
  private[crypto] val cutOffDuration =
    PositiveSeconds.fromConfig(sessionSigningKeysConfig.cutOffDuration)

  /** The duration a session signing key is retained in memory. It is defined as an AtomicReference
    * only so it can be changed for tests.
    */
  @VisibleForTesting
  private[crypto] val sessionKeyEvictionPeriod = new AtomicReference(
    PositiveSeconds.fromConfig(sessionSigningKeysConfig.keyEvictionPeriod)
  )

  /** Caches the session signing private key and corresponding signature delegation, indexed by the
    * session key ID. The removal of entries from the cache is controlled by a separate parameter,
    * [[sessionKeyEvictionPeriod]]. Given this design, there may be times when multiple valid
    * session keys live in the cache. In such cases, the newest key is always selected to sign new
    * messages.
    */
  @VisibleForTesting
  private[crypto] val sessionKeysSigningCache: Cache[Fingerprint, SessionKeyAndDelegation] =
    Scaffeine()
      // TODO(#24566): Use scheduler instead of expireAfter
      .expireAfter[Fingerprint, SessionKeyAndDelegation](
        create = (_, _) => sessionKeyEvictionPeriod.get().toFiniteDuration,
        update = (_, _, d) => d,
        read = (_, _, d) => d,
      )
      .executor(executionContext.execute(_))
      .build()

  /** Creates a delegation signature that authorizes the session key to act on behalf of the
    * long-term key.
    */
  private def createDelegationSignature(
      topologySnapshot: TopologySnapshot,
      existingKeys: Seq[SigningPublicKey],
      sessionKey: SigningPublicKey,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SignatureDelegation] =
    for {
      longTermKey <- PublicKey
        .getLatestKey(existingKeys)
        .toRight(
          SyncCryptoError
            .KeyNotAvailable(
              member,
              KeyPurpose.Signing,
              topologySnapshot.timestamp,
              Seq.empty,
            )
        )
        .toEitherT[FutureUnlessShutdown]

      validityPeriod = SignatureDelegationValidityPeriod(
        topologySnapshot.timestamp,
        sessionKeyValidityPeriod,
      )

      hash = SignatureDelegation.generateHash(
        synchronizerId,
        sessionKey.id,
        validityPeriod,
      )

      // sign the hash with the long-term key
      signature <- signPrivateApiDefault
        .sign(hash, longTermKey.fingerprint, SigningKeyUsage.ProtocolOnly)
        .leftMap[SyncCryptoError](err => SyncCryptoError.SyncCryptoSigningError(err))
      signatureDelegation <- SignatureDelegation
        .create(
          sessionKey,
          validityPeriod,
          signature,
        )
        .leftMap[SyncCryptoError](errMsg =>
          SyncCryptoError.SyncCryptoDelegationSignatureCreationError(errMsg)
        )
        .toEitherT[FutureUnlessShutdown]
    } yield signatureDelegation

  private def generateNewSessionKey(
      topologySnapshot: TopologySnapshot,
      existingKeys: Seq[SigningPublicKey],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SessionKeyAndDelegation] =
    for {
      // session keys are only used to sign protocol messages
      sessionKeyPair <- JcePrivateCrypto
        .generateSigningKeypair(sessionKeySpec, SigningKeyUsage.ProtocolOnly)
        .leftMap[SyncCryptoError](err => SyncCryptoError.SyncCryptoSessionKeyGenerationError(err))
        .toEitherT[FutureUnlessShutdown]
      // sign session key + metadata with long-term key to authorize the delegation
      signatureDelegation <- createDelegationSignature(
        topologySnapshot,
        existingKeys,
        sessionKeyPair.publicKey,
      )
      sessionKeyAndDelegation = SessionKeyAndDelegation(
        sessionKeyPair.privateKey,
        signatureDelegation,
      )
      _ = sessionKeysSigningCache
        .put(
          sessionKeyPair.publicKey.id,
          sessionKeyAndDelegation,
        )
    } yield sessionKeyAndDelegation

  /** The selection of a session key is based on its validity. If multiple options are available, we
    * retrieve the newest key. If no session key is available, we create a new one.
    */
  private def getOrCreateSessionKey(
      topologySnapshot: TopologySnapshot
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SessionKeyAndDelegation] = {
    val validSessionKeys = sessionKeysSigningCache.asMap().filter { case (_, skD) =>
      skD.signatureDelegation.isValidAt(topologySnapshot.timestamp) &&
      // If sufficient time has passed and the cut-off threshold has been reached,
      // the current signing key is no longer used, and a different or new key must be used.
      topologySnapshot.timestamp < skD.signatureDelegation.validityPeriod
        .computeCutOffTimestamp(cutOffDuration)
    }

    for {
      activeLongTermKeys <- EitherT
        .right[SyncCryptoError](topologySnapshot.signingKeys(member, SigningKeyUsage.ProtocolOnly))
      activeKeyIds = activeLongTermKeys.map(_.id)

      validSessionKeysToUse =
        validSessionKeys.filter { case (_, skD) =>
          val longTermKeyId = skD.signatureDelegation.signature.signedBy
          activeKeyIds.contains(longTermKeyId)
        }

      sessionKeyAndDelegation <- NonEmpty.from(validSessionKeysToUse.toMap) match {
        case None =>
          generateNewSessionKey(topologySnapshot, activeLongTermKeys)
        case Some(validSessionKeys) =>
          EitherT.pure[FutureUnlessShutdown, SyncCryptoError] {
            // retrieve newest key
            val (_, keyAndDelegation) = validSessionKeys.maxBy1 { case (_, skD) =>
              skD.signatureDelegation.validityPeriod.fromInclusive
            }
            keyAndDelegation
          }
      }

    } yield sessionKeyAndDelegation
  }

  override def sign(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] =
    for {
      sessionKeyAndDelegation <- getOrCreateSessionKey(topologySnapshot)
      SessionKeyAndDelegation(sessionKey, delegation) = sessionKeyAndDelegation
      signature <- signPublicApiWithSessionKeys
        .sign(hash, sessionKey, usage)
        .toEitherT[FutureUnlessShutdown]
        .leftMap[SyncCryptoError](SyncCryptoError.SyncCryptoSigningError.apply)
    } yield signature.addSignatureDelegation(delegation)

  // TODO(#22362): to be implemented
  override def verifySignature(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    ???

  // TODO(#22362): to be implemented
  override def verifySignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    ???

  // TODO(#22362): to be implemented
  override def verifyGroupSignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signers: Seq[Member],
      threshold: PositiveInt,
      groupName: String,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    ???

}

object SyncCryptoSignerWithSessionKeys {
  private[crypto] final case class SessionKeyAndDelegation(
      sessionPrivateKey: SigningPrivateKey,
      signatureDelegation: SignatureDelegation,
  )
}
