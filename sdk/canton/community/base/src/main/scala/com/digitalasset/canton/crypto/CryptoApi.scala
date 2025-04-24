// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.show.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CryptoConfig, CryptoProvider, ProcessingTimeout}
import com.digitalasset.canton.crypto.kms.KmsFactory
import com.digitalasset.canton.crypto.provider.jce.{JceCrypto, JcePureCrypto}
import com.digitalasset.canton.crypto.provider.kms.KmsPrivateCrypto
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStore,
  CryptoPrivateStoreError,
  CryptoPrivateStoreFactory,
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
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.version.{HasToByteString, ReleaseProtocolVersion}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** Wrapper class to simplify crypto dependency management */
class Crypto(
    val pureCrypto: CryptoPureApi,
    val privateCrypto: CryptoPrivateApi,
    val cryptoPrivateStore: CryptoPrivateStore,
    val cryptoPublicStore: CryptoPublicStore,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with CloseableHealthElement
    with CompositeHealthElement[String, HealthQuasiComponent]
    with HealthComponent {

  /** Helper method to generate a new signing key pair and store the public key in the public store
    * as well.
    */
  def generateSigningKey(
      keySpec: SigningKeySpec = privateCrypto.defaultSigningKeySpec,
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
      keySpec: EncryptionKeySpec = privateCrypto.defaultEncryptionKeySpec,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionPublicKey] =
    for {
      publicKey <- privateCrypto.generateEncryptionKey(keySpec, name)
      _ <- EitherT.right(cryptoPublicStore.storeEncryptionKey(publicKey, name))
    } yield publicKey

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
    with CloseableHealthComponent

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

// architecture-handbook-entry-begin: SyncCryptoApi
/** impure part of the crypto api with access to private key store and knowledge about the current
  * entity to key assoc
  */
trait SyncCryptoApi {

  def pureCrypto: CryptoPureApi

  def ipsSnapshot: TopologySnapshot

  /** Signs the given hash using the private signing key. It uses the most recent signing key with
    * the specified usage in the private store. The key usage must intersect with the provided
    * usage, but it does not need to satisfy all the provided usages.
    *
    * @param hash
    *   the hash to sign
    * @param usage
    *   restricts signing to private keys that have at least one matching usage
    */
  def sign(
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
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
    * Utility method to lookup a key on an IPS snapshot and then encrypt the given message with the
    * most suitable key for the respective key owner.
    */
  def encryptFor[M <: HasToByteString, MemberType <: Member](
      message: M,
      members: Seq[MemberType],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, (MemberType, SyncCryptoError), Map[
    MemberType,
    AsymmetricEncrypted[M],
  ]]
}
// architecture-handbook-entry-end: SyncCryptoApi

object Crypto {

  def create(
      config: CryptoConfig,
      storage: Storage,
      cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
      kmsFactory: KmsFactory,
      releaseProtocolVersion: ReleaseProtocolVersion,
      nonStandardConfig: Boolean,
      futureSupervisor: FutureSupervisor,
      clock: Clock,
      executionContext: ExecutionContext,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Crypto] =
    for {
      cryptoPublicStore <- CryptoPublicStore
        .create(storage, releaseProtocolVersion, timeouts, loggerFactory)
        .leftMap(err => show"Failed to create crypto public store: $err")
      cryptoPrivateStore <- cryptoPrivateStoreFactory
        .create(storage, releaseProtocolVersion, timeouts, loggerFactory, tracerProvider)
        .leftMap(err => show"Failed to create crypto private store: $err")
      crypto <- config.provider match {
        case CryptoProvider.Jce =>
          JceCrypto
            .create(
              config,
              cryptoPrivateStore,
              cryptoPublicStore,
              timeouts,
              loggerFactory,
            )
            .toEitherT[FutureUnlessShutdown]
        case CryptoProvider.Kms =>
          EitherT.fromEither[FutureUnlessShutdown] {
            for {
              kmsConfig <- config.kms.toRight("Missing KMS configuration for KMS crypto provider")
              cryptoSchemes <- CryptoSchemes.fromConfig(config)
              kms <- kmsFactory
                .create(
                  kmsConfig,
                  nonStandardConfig,
                  timeouts,
                  futureSupervisor,
                  tracerProvider,
                  clock,
                  loggerFactory,
                  executionContext,
                )
                .leftMap(err => s"Failed to create the KMS client: $err")
              kmsCryptoPrivateStore <- KmsCryptoPrivateStore.fromCryptoPrivateStore(
                cryptoPrivateStore
              )
              kmsPrivateCrypto <- KmsPrivateCrypto.create(
                kms,
                cryptoSchemes,
                cryptoPublicStore,
                kmsCryptoPrivateStore,
                timeouts,
                loggerFactory,
              )
              pureCrypto <- JcePureCrypto.create(
                config.copy(provider = CryptoProvider.Jce),
                loggerFactory,
              )
            } yield new Crypto(
              pureCrypto,
              kmsPrivateCrypto,
              cryptoPrivateStore,
              cryptoPublicStore,
              timeouts,
              loggerFactory,
            )
          }
      }
    } yield crypto

}
