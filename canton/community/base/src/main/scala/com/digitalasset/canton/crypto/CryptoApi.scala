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
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

/** Wrapper class to simplify crypto dependency management */
class Crypto(
    val pureCrypto: CryptoPureApi,
    val privateCrypto: CryptoPrivateApi,
    val cryptoPrivateStore: CryptoPrivateStore,
    val cryptoPublicStore: CryptoPublicStore,
    val javaKeyConverter: JavaKeyConverter,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  /** Helper method to generate a new signing key pair and store the public key in the public store as well. */
  def generateSigningKey(
      scheme: SigningKeyScheme = privateCrypto.defaultSigningKeyScheme,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SigningKeyGenerationError, SigningPublicKey] =
    for {
      publicKey <- privateCrypto.generateSigningKey(scheme, name)
      _ <- cryptoPublicStore
        .storeSigningKey(publicKey, name)
        .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.SigningPublicStoreError)
    } yield publicKey

  /** Helper method to generate a new encryption key pair and store the public key in the public store as well. */
  def generateEncryptionKey(
      scheme: EncryptionKeyScheme = privateCrypto.defaultEncryptionKeyScheme,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionPublicKey] =
    for {
      publicKey <- privateCrypto.generateEncryptionKey(scheme, name)
      _ <- cryptoPublicStore
        .storeEncryptionKey(publicKey, name)
        .leftMap[EncryptionKeyGenerationError](
          EncryptionKeyGenerationError.EncryptionPublicStoreError
        )
    } yield publicKey

  override def onClosed(): Unit = Lifecycle.close(cryptoPrivateStore, cryptoPublicStore)(logger)
}

trait CryptoPureApi
    extends EncryptionOps
    with SigningOps
    with HmacOps
    with HkdfOps
    with HashOps
    with RandomOps
trait CryptoPrivateApi extends EncryptionPrivateOps with SigningPrivateOps
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
    override def pretty: Pretty[KeyNotAvailable] = prettyOfClass(
      param("owner", _.owner),
      param("key purpose", _.keyPurpose),
      param("timestamp", _.timestamp),
      param("candidates", _.candidates),
    )
  }

  final case class SyncCryptoSigningError(error: SigningError) extends SyncCryptoError {
    override def pretty: Pretty[SyncCryptoSigningError] = prettyOfParam(_.error)
  }

  final case class SyncCryptoDecryptionError(error: DecryptionError) extends SyncCryptoError {
    override def pretty: Pretty[SyncCryptoDecryptionError] = prettyOfParam(_.error)
  }

  final case class SyncCryptoEncryptionError(error: EncryptionError) extends SyncCryptoError {
    override def pretty: Pretty[SyncCryptoEncryptionError] = prettyOfParam(_.error)
  }

  final case class StoreError(error: CryptoPrivateStoreError) extends SyncCryptoError {
    override def pretty: Pretty[StoreError] =
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
  def sign(hash: Hash)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncCryptoError, Signature]

  /** Decrypts a message using the private key of the public key given as the fingerprint. */
  def decrypt[M](encryptedMessage: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit traceContext: TraceContext): EitherT[Future, SyncCryptoError, M]

  /** Verify signature of a given owner
    *
    * Convenience method to lookup a key of a given owner, domain and timestamp and verify the result.
    */
  def verifySignature(
      hash: Hash,
      signer: Member,
      signature: Signature,
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit]

  def verifySignatures(
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit]

  /** Verifies a list of `signatures` to be produced by active members of a `mediatorGroup`,
    * counting each member's signature only once.
    * Returns `Right` when the `mediatorGroup`'s threshold is met.
    * Can be successful even if some signatures fail the check, logs the errors in that case.
    * When the threshold is not met returns `Left` with all the signature check errors.
    */
  def verifySignatures(
      hash: Hash,
      mediatorGroupIndex: MediatorGroupIndex,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit]

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
  ): EitherT[Future, (MemberType, SyncCryptoError), Map[MemberType, AsymmetricEncrypted[M]]]
}
// architecture-handbook-entry-end: SyncCryptoApi
