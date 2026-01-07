// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.show.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  BatchingConfig,
  CacheConfig,
  CryptoConfig,
  CryptoProvider,
  EncryptedPrivateStoreConfig,
  KmsConfig,
  ProcessingTimeout,
  SessionEncryptionKeyCacheConfig,
}
import com.digitalasset.canton.crypto.kms.aws.AwsKms
import com.digitalasset.canton.crypto.kms.driver.v1.DriverKms
import com.digitalasset.canton.crypto.kms.gcp.GcpKms
import com.digitalasset.canton.crypto.kms.{Kms, KmsFactory}
import com.digitalasset.canton.crypto.provider.jce.{JceCrypto, JcePureCrypto}
import com.digitalasset.canton.crypto.provider.kms.KmsPrivateCrypto
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStore,
  CryptoPrivateStoreError,
  CryptoPublicStore,
  KmsCryptoPrivateStore,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.{
  CloseableHealthComponent,
  CloseableHealthElement,
  ComponentHealthState,
  CompositeHealthElement,
  HealthComponent,
  HealthQuasiComponent,
}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.version.{HasToByteString, ReleaseProtocolVersion}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** A base trait that provides all the essential cryptographic components, offering a unified
  * interface for cryptographic operations and key management.
  *
  * This includes:
  *   - Public and private crypto APIs, providing functionality for encryption, decryption, signing,
  *     and verification.
  *   - Public and private key store APIs, responsible for managing the persistence and retrieval of
  *     cryptographic keys.
  */
sealed trait BaseCrypto extends NamedLogging {

  protected implicit val ec: ExecutionContext

  def pureCrypto: CryptoPureApi
  def privateCrypto: CryptoPrivateApi
  def cryptoPrivateStore: CryptoPrivateStore
  def cryptoPublicStore: CryptoPublicStore

  /** Helper method to generate a new signing key pair and store the public key in the public store
    * as well.
    */
  def generateSigningKey(
      keySpec: SigningKeySpec = privateCrypto.signingSchemes.keySpecs.default,
      usage: NonEmpty[Set[SigningKeyUsage]],
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningPublicKey] =
    for {
      publicKey <- privateCrypto.generateSigningKey(keySpec, usage, name)
      _ <- EitherT.right(cryptoPublicStore.storeSigningKey(publicKey, name))
    } yield publicKey

  /** Helper method to generate a new encryption key pair and store the public key in the public
    * store as well.
    */
  def generateEncryptionKey(
      keySpec: EncryptionKeySpec = privateCrypto.encryptionSchemes.keySpecs.default,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionPublicKey] =
    for {
      publicKey <- privateCrypto.generateEncryptionKey(keySpec, name)
      _ <- EitherT.right(cryptoPublicStore.storeEncryptionKey(publicKey, name))
    } yield publicKey

}

/** Wrapper class to simplify crypto dependency management. It does not validate crypto schemes
  * against the static synchronizer parameters.
  */
class Crypto private[crypto] (
    override val pureCrypto: CryptoPureApi,
    override val privateCrypto: CryptoPrivateApi,
    override val cryptoPrivateStore: CryptoPrivateStore,
    override val cryptoPublicStore: CryptoPublicStore,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends BaseCrypto
    with CloseableHealthElement
    with CompositeHealthElement[String, HealthQuasiComponent]
    with HealthComponent {

  override def onClosed(): Unit =
    LifeCycle.close(privateCrypto, cryptoPrivateStore, cryptoPublicStore)(logger)

  override def name: String = "crypto"

  setDependency("private-crypto", privateCrypto)

  override protected def combineDependentStates: ComponentHealthState =
    // Currently we only check the health of the private crypto API due to its implementation on an external KMS
    privateCrypto.getState

  override protected def initialHealthState: ComponentHealthState =
    ComponentHealthState.NotInitializedState
}

/** Similar to [[Crypto]], but includes wrappers for [[CryptoPureApi]] and [[CryptoPrivateApi]] that
  * add crypto scheme validation checks against the static synchronizer parameters.
  */
final case class SynchronizerCrypto(
    crypto: Crypto,
    staticSynchronizerParameters: StaticSynchronizerParameters,
)(override implicit val ec: ExecutionContext)
    extends BaseCrypto {

  override val pureCrypto: SynchronizerCryptoPureApi =
    new SynchronizerCryptoPureApi(staticSynchronizerParameters, crypto.pureCrypto)

  override val privateCrypto: SynchronizerCryptoPrivateApi =
    new SynchronizerCryptoPrivateApi(
      staticSynchronizerParameters,
      crypto.privateCrypto,
      crypto.timeouts,
      crypto.loggerFactory,
    )

  override val cryptoPrivateStore: CryptoPrivateStore = crypto.cryptoPrivateStore
  override val cryptoPublicStore: CryptoPublicStore = crypto.cryptoPublicStore
  override protected val loggerFactory: NamedLoggerFactory = crypto.loggerFactory
}

trait CryptoPureApi
    extends EncryptionOps
    with SigningOps
    with HmacOps
    with HashOps
    with RandomOps
    with PasswordBasedEncryptionOps

sealed trait CryptoPureApiError extends Product with Serializable with PrettyPrinting
object CryptoPureApiError {
  final case class KeyParseAndValidateError(error: String) extends CryptoPureApiError {
    override protected def pretty: Pretty[KeyParseAndValidateError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
}

trait CryptoPrivateApi
    extends EncryptionPrivateOps
    with SigningPrivateOps
    with CloseableHealthComponent {

  private[crypto] def getInitialHealthState: ComponentHealthState

}

trait CryptoPrivateStoreApi
    extends CryptoPrivateApi
    with EncryptionPrivateStoreOps
    with SigningPrivateStoreOps

sealed trait SyncCryptoError extends Product with Serializable with PrettyPrinting
object SyncCryptoError {

  final case class SyncCryptoSessionKeyGenerationError(error: SigningKeyGenerationError)
      extends SyncCryptoError {
    override protected def pretty: Pretty[SyncCryptoSessionKeyGenerationError] = prettyOfParam(
      _.error
    )
  }

  /** error thrown if there is no key available as per identity providing service */
  final case class KeyNotAvailable(
      owner: Member,
      keyPurpose: KeyPurpose,
      timestamp: CantonTimestamp,
      candidates: Seq[Fingerprint],
  ) extends SyncCryptoError {
    override protected def pretty: Pretty[KeyNotAvailable] = prettyOfClass(
      param("owner", _.owner),
      param("key purpose", _.keyPurpose),
      param("timestamp", _.timestamp),
      param("candidates", _.candidates),
    )
  }

  final case class SyncCryptoSigningError(error: SigningError) extends SyncCryptoError {
    override protected def pretty: Pretty[SyncCryptoSigningError] = prettyOfParam(_.error)
  }

  final case class SyncCryptoDecryptionError(error: DecryptionError) extends SyncCryptoError {
    override protected def pretty: Pretty[SyncCryptoDecryptionError] = prettyOfParam(_.error)
  }

  final case class SyncCryptoEncryptionError(error: EncryptionError) extends SyncCryptoError {
    override protected def pretty: Pretty[SyncCryptoEncryptionError] = prettyOfParam(_.error)
  }

  final case class StoreError(error: CryptoPrivateStoreError) extends SyncCryptoError {
    override protected def pretty: Pretty[StoreError] =
      prettyOfClass(unnamedParam(_.error))
  }

  /** Thrown when a sign message request does not support session signing keys, e.g., for a
    * non-protocol message.
    */
  final case class UnsupportedDelegationSignatureError(message: String) extends SyncCryptoError {
    override protected def pretty: Pretty[UnsupportedDelegationSignatureError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Thrown when invariant checks fail during the creation of a signature delegation. This can
    * occur if the session key or the generated signature does not follow the correct format.
    */
  final case class SyncCryptoDelegationSignatureCreationError(error: String)
      extends SyncCryptoError {
    override protected def pretty: Pretty[SyncCryptoDelegationSignatureCreationError] =
      prettyOfClass(
        unnamedParam(_.error.unquoted)
      )
  }
}

/** impure part of the crypto api with access to private key store and knowledge about the current
  * entity to key assoc
  */
trait SyncCryptoApi {

  def pureCrypto: SynchronizerCryptoPureApi

  def ipsSnapshot: TopologySnapshot

  /** Signs the given hash using the private signing key. It uses the most recent signing key with
    * the specified usage in the private store. The key usage must intersect with the provided
    * usage, but it does not need to satisfy all the provided usages.
    *
    * @param hash
    *   the hash to sign
    * @param usage
    *   restricts signing to private keys that have at least one matching usage
    * @param approximateTimestampOverride
    *   optional timestamp to use for signing. Should only be set for signatures that end up, for
    *   example, in submission requests, or of encrypted view messages, where the topology is not
    *   yet fixed, i.e., when using a topology snapshot approximation. The current local clock
    *   reading is often a suitable value. This timestamp will be used to pick a session signing key
    *   with a suitable validity period, if needed.
    */
  def sign(
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
      approximateTimestampOverride: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature]

  /** Verify signature of a given owner. Convenience method to lookup a key of a given owner,
    * synchronizer and timestamp and verify the result.
    *
    * @param usage
    *   verifies that the signature was produced with a signing key with at least one matching usage
    */
  def verifySignature(
      hash: Hash,
      signer: Member,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  def verifySignatures(
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  /** Verifies a list of `signatures` to be produced by active members of a `mediatorGroup`,
    * counting each member's signature only once. Returns `Right` when the `mediatorGroup`'s
    * threshold is met. Can be successful even if some signatures fail the check, logs the errors in
    * that case. When the threshold is not met returns `Left` with all the signature check errors.
    */
  def verifyMediatorSignatures(
      hash: Hash,
      mediatorGroupIndex: MediatorGroupIndex,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  def verifySequencerSignatures(
      hash: Hash,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  /** Decrypts a message using the private key of the public key identified by the fingerprint in
    * the AsymmetricEncrypted object.
    */
  def decrypt[M](encryptedMessage: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncCryptoError, M]

  /** Encrypts a message for the given members
    *
    * Utility method to look up a key on an IPS snapshot and then encrypt the given message with the
    * most suitable key for the respective key owner.
    *
    * @param deterministicEncryption
    *   when enabled, the same message with the same key always yields the same ciphertext. This
    *   leaks equality of messages and enables frequency analysis, so it should only be used when
    *   encrypting a one-time, short-lived session signing key.
    */
  def encryptFor[M <: HasToByteString, MemberType <: Member](
      message: M,
      members: Seq[MemberType],
      deterministicEncryption: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, (MemberType, SyncCryptoError), Map[
    MemberType,
    AsymmetricEncrypted[M],
  ]]
}

object Crypto {

  def create(
      config: CryptoConfig,
      kmsStoreCacheConfig: CacheConfig,
      sessionEncryptionKeyCacheConfig: SessionEncryptionKeyCacheConfig,
      publicKeyConversionCacheConfig: CacheConfig,
      storage: Storage,
      replicaManager: Option[ReplicaManager],
      releaseProtocolVersion: ReleaseProtocolVersion,
      futureSupervisor: FutureSupervisor,
      clock: Clock,
      executionContext: ExecutionContext,
      timeouts: ProcessingTimeout,
      batchingConfig: BatchingConfig,
      loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Crypto] = {

    def kmsClient(): EitherT[FutureUnlessShutdown, String, Kms] =
      for {
        kmsConfig <- config.kms
          .toRight("Missing KMS configuration for KMS crypto provider")
          .toEitherT[FutureUnlessShutdown]
        kms <- KmsFactory
          .create(
            kmsConfig,
            timeouts,
            futureSupervisor,
            tracerProvider,
            clock,
            loggerFactory,
            executionContext,
          )
          .leftMap(err => s"Failed to create the KMS client: $err")
          .toEitherT[FutureUnlessShutdown]
      } yield kms

    def createCryptoWithJceProvider(
        cryptoSchemes: CryptoSchemes,
        cryptoPublicStore: CryptoPublicStore,
        cryptoPrivateStore: CryptoPrivateStore,
    )(implicit
        ec: ExecutionContext
    ): EitherT[FutureUnlessShutdown, String, Crypto] =
      JceCrypto
        .create(
          config,
          cryptoSchemes,
          sessionEncryptionKeyCacheConfig,
          publicKeyConversionCacheConfig,
          cryptoPrivateStore,
          cryptoPublicStore,
          timeouts,
          loggerFactory,
        )
        .toEitherT[FutureUnlessShutdown]

    // If the supported schemes are already provided statically (e.g., for AWS or GCP), they are handled elsewhere.
    // This method is used only when dealing with a KMS driver, in which case the supported schemes are determined
    // dynamically by 'querying' the driver. The resulting set of schemes si to be used only by the (KMS)
    // private crypto API.
    def resolveDriverKmsSupportedSchemes(
        cryptoSchemes: CryptoSchemes,
        kms: Kms,
    ): EitherT[FutureUnlessShutdown, String, CryptoSchemes] =
      kms match {
        case kms: DriverKms =>
          CryptoSchemes
            .selectKmsSchemes(cryptoSchemes, kms)
            .toEitherT[FutureUnlessShutdown]
        case _ =>
          EitherT
            .leftT[FutureUnlessShutdown, CryptoSchemes](
              "expected a driver KMS client"
            )
      }

    def createCryptoWithKmsProvider(
        kms: Kms,
        cryptoSchemes: CryptoSchemes,
        kmsSchemes: CryptoSchemes,
        cryptoPublicStore: CryptoPublicStore,
    )(implicit
        ec: ExecutionContext
    ): EitherT[FutureUnlessShutdown, String, Crypto] = {
      val kmsCryptoPrivateStore = KmsCryptoPrivateStore.create(
        storage,
        kms,
        kmsStoreCacheConfig,
        timeouts,
        loggerFactory,
      )
      val kmsPrivateCrypto = KmsPrivateCrypto
        .create(
          kms,
          kmsSchemes.signingSchemes,
          kmsSchemes.encryptionSchemes,
          cryptoPublicStore,
          kmsCryptoPrivateStore,
          timeouts,
          loggerFactory,
        )
      for {
        pureCrypto <- JcePureCrypto
          .create(
            config.copy(provider = CryptoProvider.Jce),
            sessionEncryptionKeyCacheConfig,
            publicKeyConversionCacheConfig,
            cryptoSchemes,
            loggerFactory,
          )
          .toEitherT[FutureUnlessShutdown]
      } yield new Crypto(
        pureCrypto,
        kmsPrivateCrypto,
        kmsCryptoPrivateStore,
        cryptoPublicStore,
        timeouts,
        loggerFactory,
      )
    }

    // Creates a [[Crypto]] instance with a KMS provider whose supported schemes are announced statically
    // (i.e., AWS KMS or GCP KMS).
    def createCryptoWithStaticKmsProvider(
        kmsSupportedSchemes: Kms.SupportedSchemes,
        cryptoSchemes: CryptoSchemes,
        cryptoPublicStore: CryptoPublicStore,
    ): EitherT[FutureUnlessShutdown, String, Crypto] =
      for {
        staticKmsSchemes <-
          CryptoSchemes
            .selectKmsSchemes(cryptoSchemes, kmsSupportedSchemes)
            .toEitherT[FutureUnlessShutdown]
        kmsClient <- kmsClient()
        kmsCryptoStatic <- ResourceUtil.withResourceCloseOnlyOnError(kmsClient) { kms =>
          createCryptoWithKmsProvider(
            kms,
            cryptoSchemes,
            staticKmsSchemes,
            cryptoPublicStore,
          )
        }
      } yield kmsCryptoStatic

    // Creates a [[Crypto]] instance using a KMS driver and its supported schemes.
    def createCryptoWithDriverKmsProvider(
        cryptoSchemes: CryptoSchemes,
        cryptoPublicStore: CryptoPublicStore,
    ): EitherT[FutureUnlessShutdown, String, Crypto] =
      kmsClient().flatMap(
        ResourceUtil.withResourceCloseOnlyOnError(_) { kms =>
          for {
            staticKmsSchemes <- resolveDriverKmsSupportedSchemes(cryptoSchemes, kms)
            kmsCryptoDriver <- createCryptoWithKmsProvider(
              kms,
              cryptoSchemes,
              staticKmsSchemes,
              cryptoPublicStore,
            )
          } yield kmsCryptoDriver
        }
      )

    for {
      // initial selection of schemes by intersecting those supported by the provider with
      // those specified in the Canton configuration, to be used by the pure (public) crypto API.
      cryptoSchemes <- CryptoSchemes.fromConfig(config).toEitherT[FutureUnlessShutdown]
      cryptoPublicStore <- CryptoPublicStore
        .create(storage, releaseProtocolVersion, timeouts, loggerFactory)
        .leftMap(err => show"Failed to create crypto public store: $err")

      crypto <- config.provider match {
        case CryptoProvider.Jce =>
          config.privateKeyStore.encryption match {
            case Some(EncryptedPrivateStoreConfig.Kms(wrapperKeyId, reverted)) =>
              kmsClient().flatMap(
                ResourceUtil.withResourceCloseOnlyOnError(_) { kms =>
                  for {
                    cryptoPrivateStore <- CryptoPrivateStore
                      .createEncrypted(
                        storage,
                        kms,
                        wrapperKeyId,
                        reverted,
                        replicaManager,
                        releaseProtocolVersion,
                        timeouts,
                        batchingConfig,
                        loggerFactory,
                      )
                      .leftMap(err => show"Failed to create crypto private store: $err")
                    jceCrypto <- createCryptoWithJceProvider(
                      cryptoSchemes,
                      cryptoPublicStore,
                      cryptoPrivateStore,
                    )
                  } yield jceCrypto
                }
              )
            case None =>
              for {
                cryptoPrivateStore <- CryptoPrivateStore
                  .create(storage, releaseProtocolVersion, timeouts, batchingConfig, loggerFactory)
                  .leftMap(err => show"Failed to create crypto private store: $err")
                jceCrypto <- createCryptoWithJceProvider(
                  cryptoSchemes,
                  cryptoPublicStore,
                  cryptoPrivateStore,
                )
              } yield jceCrypto
          }
        case CryptoProvider.Kms =>
          for {
            kmsConfig <- config.kms
              .toRight("Missing KMS configuration for KMS crypto provider")
              .toEitherT[FutureUnlessShutdown]
            // From the selected crypto schemes, we check whether the chosen default is supported by the KMS,
            // and from the full list of allowed schemes, we keep only those supported by the KMS.
            // This set of schemes is intended to be used only by the (KMS) private crypto API.
            // The selection is done before initializing the KMS client to catch any unsupported schemes,
            // except when running a KMS driver. In that case, we only know which schemes are supported after
            // initializing the driver, whereas for other KMS types, the supported schemes are defined statically
            // (e.g., AWS and GCP KMS)
            kmsCrypto <- kmsConfig match {
              case _: KmsConfig.Driver =>
                createCryptoWithDriverKmsProvider(cryptoSchemes, cryptoPublicStore)
              case _: KmsConfig.Aws =>
                createCryptoWithStaticKmsProvider(AwsKms, cryptoSchemes, cryptoPublicStore)
              case _: KmsConfig.Gcp =>
                createCryptoWithStaticKmsProvider(GcpKms, cryptoSchemes, cryptoPublicStore)
            }
          } yield kmsCrypto
      }
    } yield crypto
  }

}
