// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, SessionSigningKeysConfig}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.EncryptionAlgorithmSpec.RsaOaepSha256
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.SymmetricKeyScheme.Aes128Gcm
import com.digitalasset.canton.crypto.provider.jce.{JcePrivateCrypto, JcePureCrypto}
import com.digitalasset.canton.crypto.signer.SyncCryptoSignerWithSessionKeys.{
  PendingUsableSessionKeysAndMetadata,
  SessionKeyAndDelegation,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.Success

/** Defines the methods for protocol message signing using a session signing key. This requires
  * signatures to include information about which session key is being used, as well as an
  * authorization by a long-term key through an additional signature. This extra signature covers
  * the session key, its validity period, and the synchronizer for which it is valid. This allows us
  * to use the session key, within a specific time frame and synchronizer, to sign protocol messages
  * instead of using the long-term key. Session keys are intended to be used with a KMS/HSM-based
  * provider to reduce the number of signing calls and, consequently, lower the latency costs
  * associated with such external key management services.
  *
  * @param signPrivateApiWithLongTermKeys
  *   The crypto private API used to sign session signing keys, creating a signature delegation with
  *   a long-term key.
  */
class SyncCryptoSignerWithSessionKeys(
    synchronizerId: SynchronizerId,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    member: Member,
    signPrivateApiWithLongTermKeys: SigningPrivateOps,
    sessionSigningKeysConfig: SessionSigningKeysConfig,
    futureSupervisor: FutureSupervisor,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncCryptoSigner
    with FlagCloseable
    with HasCloseContext {

  /** The software-based crypto public API that is used to sign protocol messages with a session
    * signing key (generated in software). Except for the signing scheme, when signing with session
    * keys is enabled, all other schemes are not needed. Therefore, we use fixed schemes (i.e.
    * placeholders) for the other crypto parameters.
    *
    * //TODO(#23731): Split up pure crypto into smaller modules and only use the signing module here
    */
  private lazy val signPublicApiSoftwareBased: SynchronizerCryptoPureApi = {
    val pureCryptoForSessionKeys = new JcePureCrypto(
      defaultSymmetricKeyScheme = Aes128Gcm, // not used
      defaultSigningAlgorithmSpec = sessionSigningKeysConfig.signingAlgorithmSpec,
      supportedSigningAlgorithmSpecs =
        NonEmpty.mk(Set, sessionSigningKeysConfig.signingAlgorithmSpec),
      defaultEncryptionAlgorithmSpec = RsaOaepSha256, // not used
      supportedEncryptionAlgorithmSpecs = NonEmpty.mk(Set, RsaOaepSha256), // not used
      defaultHashAlgorithm = Sha256, // not used
      defaultPbkdfScheme = PbkdfScheme.Argon2idMode1, // not used
      loggerFactory = loggerFactory,
    )

    new SynchronizerCryptoPureApi(staticSynchronizerParameters, pureCryptoForSessionKeys)
  }

  /** The user-configured validity period of a session signing key. */
  private val sessionKeyValidityDuration =
    PositiveSeconds.fromConfig(sessionSigningKeysConfig.keyValidityDuration)

  /** The key specification for the session signing keys. */
  private val sessionKeySpec = sessionSigningKeysConfig.signingKeySpec

  /** A cut-off duration that determines when the key should stop being used to prevent signature
    * verification failures due to unpredictable sequencing timestamps. It is also used to tweak the
    * validity period of a key from which a key is considered valid (i.e., ts - cutOff/2 to ts + x -
    * cutOff/2), allowing closely decreasing timestamps to still be signed with the same key.
    */
  @VisibleForTesting
  private[crypto] val cutOffDuration =
    PositiveSeconds.fromConfig(sessionSigningKeysConfig.cutOffDuration)

  /** The duration a session signing key is retained in memory. It is defined as an AtomicReference
    * only so it can be changed for tests.
    */
  @VisibleForTesting
  private[crypto] val sessionKeyEvictionPeriod = new AtomicReference(
    sessionSigningKeysConfig.keyEvictionPeriod.underlying
  )

  /** Tracks pending new session signing keys. Each session key has an associated validity period
    * and a corresponding long-term key (identified by a fingerprint), both used to generate a
    * signature delegation.
    */
  @VisibleForTesting
  private[crypto] val pendingRequests: TrieMap[
    (SignatureDelegationValidityPeriod, Fingerprint),
    PromiseUnlessShutdown[Option[SessionKeyAndDelegation]],
  ] = TrieMap.empty

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
        create = (_, _) => sessionKeyEvictionPeriod.get(),
        update = (_, _, d) => d,
        read = (_, _, d) => d,
      )
      .executor(executionContext.execute(_))
      .build()

  /** To control access to the [[sessionKeysSigningCache]] and the [[pendingRequests]]. */
  private val lock = new Object()

  /** Creates a delegation signature that authorizes the session key to act on behalf of the
    * long-term key.
    */
  private def createDelegationSignature(
      validityPeriod: SignatureDelegationValidityPeriod,
      longTermKey: SigningPublicKey,
      sessionKey: SigningPublicKey,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SignatureDelegation] =
    for {
      // sign the hash with the long-term key
      signature <- signPrivateApiWithLongTermKeys
        .sign(
          SignatureDelegation.generateHash(
            synchronizerId,
            sessionKey,
            validityPeriod,
          ),
          longTermKey.fingerprint,
          SigningKeyUsage.ProtocolOnly,
        )
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

  private def isUsableDelegation(
      timestamp: CantonTimestamp,
      validityPeriod: SignatureDelegationValidityPeriod,
  ): Boolean =
    validityPeriod.covers(timestamp) &&
      // If sufficient time has passed and the cut-off threshold has been reached,
      // the current signing key is no longer used, and a different or new key must be used.
      timestamp < validityPeriod
        .computeCutOffTimestamp(cutOffDuration)

  private def signingLongTermKeyIsValid(
      activeLongTermKeyIds: Seq[Fingerprint],
      signedBy: Fingerprint,
  ) =
    activeLongTermKeyIds.contains(signedBy)

  private def generateNewSessionKey(
      validityPeriod: SignatureDelegationValidityPeriod,
      longTermKey: SigningPublicKey,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SessionKeyAndDelegation] =
    // no one is creating it yet, create it ourselves
    for {
      // session keys are only used to sign protocol messages
      sessionKeyPair <- JcePrivateCrypto
        .generateSigningKeypair(sessionKeySpec, SigningKeyUsage.ProtocolOnly)
        .leftMap[SyncCryptoError] { err =>
          SyncCryptoError.SyncCryptoSessionKeyGenerationError(err)
        }
        .toEitherT[FutureUnlessShutdown]
      // sign session key + metadata with long-term key to authorize the delegation
      signatureDelegation <- createDelegationSignature(
        validityPeriod,
        longTermKey,
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

  private def selectLongTermKeyAndValidityPeriod(
      topologySnapshot: TopologySnapshot,
      activeLongTermKeys: NonEmpty[Seq[SigningPublicKey]],
  ): (SigningPublicKey, SignatureDelegationValidityPeriod) = {
    val longTermKey = PublicKey
      .getLatestKey(activeLongTermKeys)

    /* If the session signing key created for a signing request at timestamp ts is valid from
     * ts-cutoff/2 to ts+x-cutoff/2, then if there is a sequence of signature request timestamps in the following
     * order ts, ts-1us , ts-2us, ts-3us, ... ts-n, we do not create n session keys, but rather n / cutoff.
     * Although not optional this a better approach than setting the validity period from ts to ts+x in terms
     * of number of keys created.
     */
    val margin = cutOffDuration.unwrap.dividedBy(2) // cuttoff/2
    val validityStart =
      // TODO(#25524): Add unit test to check that this IllegalArgumentException is correctly thrown
      Either
        .catchOnly[IllegalArgumentException](
          topologySnapshot.timestamp.minus(margin)
        )
        .getOrElse(CantonTimestamp.MinValue)
    val validityPeriod = SignatureDelegationValidityPeriod(
      validityStart,
      sessionKeyValidityDuration,
    )
    (longTermKey, validityPeriod)
  }

  /** The selection of a session key is based on its validity. If multiple options are available, we
    * retrieve the newest key. If no session key is available, we create a new one or wait if
    * another is already being created.
    */
  private def getSessionKey(
      topologySnapshot: TopologySnapshot,
      activeLongTermKeys: NonEmpty[Seq[SigningPublicKey]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SessionKeyAndDelegation] = {

    val activeLongTermKeyIds = activeLongTermKeys.map(_.id)

    val sessionKeyOrGenerationData = blocking(lock.synchronized {
      // get hold of all existing or pending session keys
      val pendingSessionKeys = pendingRequests.toMap
      val keysInCache = sessionKeysSigningCache.asMap().values.toSeq

      // check if there is a session key in the cache that is valid and can be used
      val validUsableSessionKeysInCache = keysInCache.filter { skD =>
        isUsableDelegation(topologySnapshot.timestamp, skD.signatureDelegation.validityPeriod) &&
        signingLongTermKeyIsValid(activeLongTermKeyIds, skD.signatureDelegation.signature.signedBy)
      }

      NonEmpty.from(validUsableSessionKeysInCache) match {
        case None =>
          // find if there is a pending session key that is valid and can be used
          val validUsablePendingRequests =
            pendingSessionKeys.view.filterKeys { case (validityPeriod, signedBy) =>
              isUsableDelegation(topologySnapshot.timestamp, validityPeriod) &&
              signingLongTermKeyIsValid(activeLongTermKeyIds, signedBy)
            }.toMap

          val (longTermKey, validityPeriod) =
            selectLongTermKeyAndValidityPeriod(topologySnapshot, activeLongTermKeys)

          // if there are no pending keys valid and usable, we add a promise to the [[pendingRequests]] map
          // and store this information in the [[PendingValidSessionKeysAndMetadata]].
          val promiseO = Option
            .when(validUsablePendingRequests.isEmpty) {
              val promise: PromiseUnlessShutdown[Option[SessionKeyAndDelegation]] =
                mkPromise[Option[SessionKeyAndDelegation]](
                  s"sync-crypto-signer-pending-requests-$validityPeriod",
                  futureSupervisor,
                )
              pendingRequests.put((validityPeriod, longTermKey.id), promise).discard
              promise
            }

          Left(
            PendingUsableSessionKeysAndMetadata(
              promiseO,
              validUsablePendingRequests,
              longTermKey,
              validityPeriod,
            )
          )
        // there is a usable and valid session key in the cache
        case Some(validUsableSessionKeysInCacheNE) =>
          // retrieve newest key
          Right(validUsableSessionKeysInCacheNE.maxBy1 { skD =>
            skD.signatureDelegation.validityPeriod.fromInclusive
          })
      }
    })

    // based on result of synchronized block, either return cached key or wait/generate
    sessionKeyOrGenerationData match {
      case Left(metadata: PendingUsableSessionKeysAndMetadata) =>
        metadata.pendingSessionKeyGenerationPromiseO match {
          // no one else is generating a key yet — we are responsible for generating it
          case Some(pendingSessionKeyGenerationPromise) =>
            generateNewSessionKey(
              metadata.validityPeriod,
              metadata.longTermKey,
            ).thereafter { result =>
              pendingRequests.remove((metadata.validityPeriod, metadata.longTermKey.id)).discard
              // maps an AbortedDueToShutdown to None, indicating to other signing calls that this session signing key
              // will not be available.
              result match {
                case Success(UnlessShutdown.Outcome(Right(sessionKeyAndDelegation))) =>
                  pendingSessionKeyGenerationPromise.outcome_(Some(sessionKeyAndDelegation))
                case _ => pendingSessionKeyGenerationPromise.outcome_(None)
              }
            }
          case None =>
            // someone else is already creating a new session key, so we wait
            val futures = metadata.validUsablePendingRequests.values.map(_.futureUS.unwrap).toSeq

            // wait for the first future to complete
            val first = FutureUnlessShutdown(Future.firstCompletedOf(futures))
            EitherT(first.transformWith {
              case Success(UnlessShutdown.Outcome(Some(sessionKeyAndDelegation))) =>
                FutureUnlessShutdown.pure(Right(sessionKeyAndDelegation))
              case _ => getSessionKey(topologySnapshot, activeLongTermKeys).value
            })
        }
      case Right(sessionKeyAndDelegation) =>
        EitherT.pure[FutureUnlessShutdown, SyncCryptoError](sessionKeyAndDelegation)
    }
  }

  override def sign(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] =
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        usage == SigningKeyUsage.ProtocolOnly,
        (),
        SyncCryptoError.UnsupportedDelegationSignatureError(
          s"Session signing keys are not supposed to be used for non-protocol messages. Requested usage: $usage"
        ),
      )
      activeLongTermKeys <- EitherT.right(
        topologySnapshot
          .signingKeys(member, SigningKeyUsage.ProtocolOnly)
      )
      activeLongTermKeysNE <- EitherT.fromEither[FutureUnlessShutdown](
        NonEmpty
          .from(activeLongTermKeys)
          .toRight(
            SyncCryptoError
              .KeyNotAvailable(
                member,
                KeyPurpose.Signing,
                topologySnapshot.timestamp,
                activeLongTermKeys.map(_.fingerprint),
              )
          )
      )
      sessionKeyAndDelegation <- getSessionKey(topologySnapshot, activeLongTermKeysNE)
      SessionKeyAndDelegation(sessionKey, delegation) = sessionKeyAndDelegation
      signature <- signPublicApiSoftwareBased
        .sign(hash, sessionKey, usage)
        .toEitherT[FutureUnlessShutdown]
        .leftMap[SyncCryptoError](SyncCryptoError.SyncCryptoSigningError.apply)
    } yield signature.addSignatureDelegation(delegation)

}

object SyncCryptoSignerWithSessionKeys {
  private[crypto] final case class SessionKeyAndDelegation(
      sessionPrivateKey: SigningPrivateKey,
      signatureDelegation: SignatureDelegation,
  )

  private type PendingSessionKeysMap = Map[
    (SignatureDelegationValidityPeriod, Fingerprint),
    PromiseUnlessShutdown[Option[SyncCryptoSignerWithSessionKeys.SessionKeyAndDelegation]],
  ]

  // metadata used to track whether we need to generate a new session key, or wait for a pending one.
  private final case class PendingUsableSessionKeysAndMetadata(
      // if no valid pending session key exists, we will create this promise to notify others
      pendingSessionKeyGenerationPromiseO: Option[
        PromiseUnlessShutdown[Option[SessionKeyAndDelegation]]
      ],
      // valid and usable pending session keys that are already being generated by others
      validUsablePendingRequests: PendingSessionKeysMap,
      longTermKey: SigningPublicKey,
      validityPeriod: SignatureDelegationValidityPeriod,
  )
}
