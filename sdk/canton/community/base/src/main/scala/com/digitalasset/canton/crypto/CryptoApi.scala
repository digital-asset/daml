// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStore,
  CryptoPrivateStoreError,
  CryptoPublicStore,
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
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
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

  /** Helper method to generate a new signing key pair and store the public key in the public store as well. */
  def generateSigningKey(
      keySpec: SigningKeySpec = privateCrypto.defaultSigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.All,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningPublicKey] =
    for {
      publicKey <- privateCrypto.generateSigningKey(keySpec, usage, name)
      _ <- EitherT.right(cryptoPublicStore.storeSigningKey(publicKey, name))
    } yield publicKey

  /** Helper method to generate a new encryption key pair and store the public key in the public store as well. */
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
}

// TODO(i8808): consider changing `encryptFor` API to
//  `def encryptFor(message: ByteString, member: Member): EitherT[Future, SyncCryptoError, ByteString]`
// architecture-handbook-entry-begin: SyncCryptoApi
/** impure part of the crypto api with access to private key store and knowledge about the current entity to key assoc */
trait SyncCryptoApi {

  def pureCrypto: CryptoPureApi

  def ipsSnapshot: TopologySnapshot

  /** Signs the given hash using the private signing key. */
  def sign(
      hash: Hash
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature]

  /** Verify signature of a given owner
    *
    * Convenience method to lookup a key of a given owner, domain and timestamp and verify the result.
    */
  def verifySignature(
      hash: Hash,
      signer: Member,
      signature: Signature,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  def verifySignatures(
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  /** Verifies a list of `signatures` to be produced by active members of a `mediatorGroup`,
    * counting each member's signature only once.
    * Returns `Right` when the `mediatorGroup`'s threshold is met.
    * Can be successful even if some signatures fail the check, logs the errors in that case.
    * When the threshold is not met returns `Left` with all the signature check errors.
    */
  def verifyMediatorSignatures(
      hash: Hash,
      mediatorGroupIndex: MediatorGroupIndex,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  def verifySequencerSignatures(
      hash: Hash,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  /** This verifies that at least one of the signature is a valid sequencer signature.
    * In particular, it does not respect the participant trust threshold.
    * This should be used only in the context of reassignment where the concept of cross-domain
    * proof of sequencing is not fully fleshed out.
    *
    * TODO(#12410) Remove this method and respect trust threshold
    */
  def unsafePartialVerifySequencerSignatures(
      hash: Hash,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  /** Decrypts a message using the private key of the public key identified by the fingerprint
    * in the AsymmetricEncrypted object.
    */
  def decrypt[M](encryptedMessage: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncCryptoError, M]

  /** Encrypts a message for the given members
    *
    * Utility method to lookup a key on an IPS snapshot and then encrypt the given message with the
    * most suitable key for the respective key owner.
    */
  def encryptFor[M <: HasVersionedToByteString, MemberType <: Member](
      message: M,
      members: Seq[MemberType],
      version: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, (MemberType, SyncCryptoError), Map[
    MemberType,
    AsymmetricEncrypted[M],
  ]]
}
// architecture-handbook-entry-end: SyncCryptoApi
