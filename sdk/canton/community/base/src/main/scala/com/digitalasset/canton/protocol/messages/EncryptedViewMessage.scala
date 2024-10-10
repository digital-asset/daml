// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.DecryptionError.InvariantViolation
import com.digitalasset.canton.crypto.SyncCryptoError.SyncCryptoDecryptionError
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError.FailedToReadKey
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPublicStore}
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.EncryptedView.checkEncryptionSpec
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageError.SyncCryptoDecryptError
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseRequiredNonEmpty}
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** An encrypted [[com.digitalasset.canton.data.ViewTree]] together with its [[com.digitalasset.canton.data.ViewType]].
  * The correspondence is encoded via a path-dependent type.
  * The type parameter `VT` exposes a upper bound on the type of view types that may be contained.
  *
  * The view tree is compressed before encryption.
  */
// This is not a case class due to the type dependency between `viewType` and `viewTree`.
// We therefore implement the case class boilerplate stuff to the extent needed.
sealed trait EncryptedView[+VT <: ViewType] extends Product with Serializable {
  val viewType: VT
  val viewTree: Encrypted[EncryptedView.CompressedView[viewType.View]]

  override def productArity: Int = 1
  override def productElement(n: Int): Any = n match {
    case 0 => viewTree
    case _ => throw new IndexOutOfBoundsException(s"Index out of range: $n")
  }
  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  override def canEqual(that: Any): Boolean = that.isInstanceOf[EncryptedView[?]]
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.IsInstanceOf",
      "org.wartremover.warts.Null",
    )
  )
  override def equals(that: Any): Boolean =
    if (this eq that.asInstanceOf[Object]) true
    else if (!that.isInstanceOf[EncryptedView[?]]) false
    else {
      val other = that.asInstanceOf[EncryptedView[ViewType]]
      val thisViewTree = this.viewTree
      if (thisViewTree eq null) other.viewTree eq null else thisViewTree == other.viewTree
    }
  override def hashCode(): Int = scala.runtime.ScalaRunTime._hashCode(this)

  /** Cast the type parameter to the given argument's [[com.digitalasset.canton.data.ViewType]]
    * provided that the argument is the same as [[viewType]]
    * @return [[scala.None$]] if `desiredViewType` does not equal [[viewType]].
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def select(desiredViewType: ViewType): Option[EncryptedView[desiredViewType.type]] =
    // Unfortunately, there doesn't seem to be a way to convince Scala's type checker that the two types must be equal.
    if (desiredViewType == viewType) Some(this.asInstanceOf[EncryptedView[desiredViewType.type]])
    else None

  /** Indicative size for pretty printing */
  def sizeHint: Int

}
object EncryptedView {
  def apply[VT <: ViewType](
      aViewType: VT
  )(aViewTree: Encrypted[CompressedView[aViewType.View]]): EncryptedView[VT] =
    new EncryptedView[VT] {
      override val viewType: aViewType.type = aViewType
      override val viewTree = aViewTree
      override lazy val sizeHint: Int = aViewTree.ciphertext.size
    }

  def compressed[VT <: ViewType](
      encryptionOps: EncryptionOps,
      viewKey: SymmetricKey,
      aViewType: VT,
  )(aViewTree: aViewType.View): Either[EncryptionError, EncryptedView[VT]] =
    encryptionOps
      .encryptWith(CompressedView(aViewTree), viewKey)
      .map(apply(aViewType))

  def decrypt[VT <: ViewType](
      encryptionOps: EncryptionOps,
      viewKey: SymmetricKey,
      encrypted: EncryptedView[VT],
  )(
      deserialize: ByteString => Either[DeserializationError, encrypted.viewType.View]
  ): Either[DecryptionError, encrypted.viewType.View] =
    encryptionOps
      .decryptWith(encrypted.viewTree, viewKey)(
        CompressedView.fromByteString[encrypted.viewType.View](deserialize)(_)
      )
      .map(_.value)

  /** Wrapper class to compress the view before encrypting it.
    *
    * This class's methods are essentially private to [[EncryptedView]]
    * because compression is in theory non-deterministic (the gzip format can store a timestamp that is ignored by decryption)
    * and we want to avoid that this is applied to [[com.digitalasset.canton.serialization.HasCryptographicEvidence]]
    * instances.
    */
  final case class CompressedView[+V <: HasToByteString] private (value: V)
      extends HasToByteString {
    override def toByteString: ByteString =
      ByteStringUtil.compressGzip(value.toByteString)
  }

  object CompressedView {
    private[EncryptedView] def apply[V <: HasToByteString](value: V): CompressedView[V] =
      new CompressedView(value)

    private[EncryptedView] def fromByteString[V <: HasToByteString](
        deserialize: ByteString => Either[DeserializationError, V]
    )(bytes: ByteString): Either[DeserializationError, CompressedView[V]] =
      // TODO(i10428) Make sure that this view does not explode into an arbitrarily large object
      ByteStringUtil
        .decompressGzip(bytes, maxBytesLimit = None)
        .flatMap(deserialize)
        .map(CompressedView(_))
  }

  def checkEncryptionSpec(
      cryptoPublicStore: CryptoPublicStore,
      keyId: Fingerprint,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
      allowedEncryptionSpecs: RequiredEncryptionSpecs,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, DecryptionError, Unit] =
    for {
      encryptionKey <- EitherT.right(
        cryptoPublicStore
          .findEncryptionKeyIdByFingerprint(keyId)
          .value
      )
      _ <- encryptionKey match {
        case Some(encPubKey) =>
          (if (!allowedEncryptionSpecs.keys.contains(encPubKey.keySpec))
             Left(
               DecryptionError.UnsupportedKeySpec(
                 encPubKey.keySpec,
                 allowedEncryptionSpecs.keys,
               )
             )
           else if (
             !encryptionAlgorithmSpec.supportedEncryptionKeySpecs.contains(encPubKey.keySpec)
           )
             Left(
               DecryptionError.UnsupportedKeySpec(
                 encPubKey.keySpec,
                 encryptionAlgorithmSpec.supportedEncryptionKeySpecs,
               )
             )
           else Right(())).toEitherT[FutureUnlessShutdown]
        case None =>
          EitherT.leftT[FutureUnlessShutdown, Unit](
            DecryptionError.InvalidEncryptionKey(s"Encryption key $keyId not found")
          )
      }
      _ <- EitherT
        .cond[FutureUnlessShutdown](
          allowedEncryptionSpecs.algorithms.contains(encryptionAlgorithmSpec),
          (),
          DecryptionError.UnsupportedAlgorithmSpec(
            encryptionAlgorithmSpec,
            allowedEncryptionSpecs.algorithms,
          ),
        )
        .leftWiden[DecryptionError]
    } yield ()

}

/** An encrypted view message.
  *
  * See [[https://engineering.da-int.net/docs/platform-architecture-handbook/arch/canton/tx-data-structures.html#transaction-hashes-and-views]]
  * The view message encrypted with symmetric key that is derived from the view's randomness.
  *
  * @param viewHash   Transaction view hash in plain text - included such that the recipient can prove to a 3rd party
  *                   that it has correctly decrypted the `viewTree`
  * @param sessionKeys a sequence of encrypted random values to each recipient of the view.
  *                   These values are encrypted and are used to derive the symmetric session key for the view.
  *                   Instead of sending a <SymmetricKey>, which could cause formatting issues
  *                   (e.g. different participants with different providers and, therefore, different key formats),
  *                   we send an encrypted <SecureRandomness>.
  */
final case class EncryptedViewMessage[+VT <: ViewType](
    submittingParticipantSignature: Option[Signature],
    viewHash: ViewHash,
    sessionKeys: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]],
    encryptedView: EncryptedView[VT],
    override val domainId: DomainId,
    viewEncryptionScheme: SymmetricKeyScheme,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      EncryptedViewMessage.type
    ]
) extends UnsignedProtocolMessage {

  @transient override protected lazy val companionObj: EncryptedViewMessage.type =
    EncryptedViewMessage

  def viewType: VT = encryptedView.viewType

  def copy[A <: ViewType](
      submittingParticipantSignature: Option[Signature] = this.submittingParticipantSignature,
      viewHash: ViewHash = this.viewHash,
      sessionKeyRandomness: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]] = this.sessionKeys,
      encryptedView: EncryptedView[A] = this.encryptedView,
      domainId: DomainId = this.domainId,
      viewEncryptionScheme: SymmetricKeyScheme = this.viewEncryptionScheme,
  ): EncryptedViewMessage[A] = new EncryptedViewMessage(
    submittingParticipantSignature,
    viewHash,
    sessionKeyRandomness,
    encryptedView,
    domainId,
    viewEncryptionScheme,
  )(representativeProtocolVersion)

  private def toProtoV30: v30.EncryptedViewMessage = v30.EncryptedViewMessage(
    viewTree = encryptedView.viewTree.ciphertext,
    encryptionScheme = viewEncryptionScheme.toProtoEnum,
    submittingParticipantSignature = submittingParticipantSignature.map(_.toProtoV30),
    viewHash = viewHash.toProtoPrimitive,
    sessionKeyLookup = sessionKeys.map(EncryptedViewMessage.serializeSessionKeyEntry),
    domainId = domainId.toProtoPrimitive,
    viewType = viewType.toProtoEnum,
  )

  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.EncryptedViewMessage(toProtoV30)

  protected def updateView[VT2 <: ViewType](
      newView: EncryptedView[VT2]
  ): EncryptedViewMessage[VT2] = copy(encryptedView = newView)

  def toByteString: ByteString = toProtoV30.toByteString

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], VT2 <: ViewType](
      f: EncryptedView[VT] => F[EncryptedView[VT2]]
  )(implicit F: Functor[F]): F[EncryptedViewMessage[VT2]] =
    F.map(f(encryptedView)) { newEncryptedView =>
      if (newEncryptedView eq encryptedView) this.asInstanceOf[EncryptedViewMessage[VT2]]
      else updateView(newEncryptedView)
    }

  override def pretty: Pretty[EncryptedViewMessage.this.type] = prettyOfClass(
    param("view hash", _.viewHash),
    param("view type", _.viewType),
    param("size", _.encryptedView.sizeHint),
  )
}

object EncryptedViewMessage extends HasProtocolVersionedCompanion[EncryptedViewMessage[ViewType]] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v32)(v30.EncryptedViewMessage)(
      supportedProtoVersion(_)(EncryptedViewMessage.fromProto),
      _.toByteString,
    )
  )

  def apply[VT <: ViewType](
      submittingParticipantSignature: Option[Signature],
      viewHash: ViewHash,
      sessionKeys: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]],
      encryptedView: EncryptedView[VT],
      domainId: DomainId,
      viewEncryptionScheme: SymmetricKeyScheme,
      protocolVersion: ProtocolVersion,
  ): EncryptedViewMessage[VT] = EncryptedViewMessage(
    submittingParticipantSignature,
    viewHash,
    sessionKeys,
    encryptedView,
    domainId,
    viewEncryptionScheme,
  )(protocolVersionRepresentativeFor(protocolVersion))

  private def serializeSessionKeyEntry(
      encryptedSessionKey: AsymmetricEncrypted[SecureRandomness]
  ): v30.SessionKeyLookup =
    v30.SessionKeyLookup(
      sessionKeyEncrypted = encryptedSessionKey.ciphertext,
      encryptionAlgorithmSpec = encryptedSessionKey.encryptionAlgorithmSpec.toProtoEnum,
      fingerprint = encryptedSessionKey.encryptedFor.toProtoPrimitive,
    )

  private def deserializeSessionKeyEntry(
      sessionKeyLookup: v30.SessionKeyLookup
  ): ParsingResult[AsymmetricEncrypted[SecureRandomness]] =
    for {
      fingerprint <- Fingerprint.fromProtoPrimitive(sessionKeyLookup.fingerprint)
      encryptionAlgorithmSpec <- EncryptionAlgorithmSpec.fromProtoEnum(
        "encryptionAlgorithmSpec",
        sessionKeyLookup.encryptionAlgorithmSpec,
      )
      sessionKeyEncrypted = sessionKeyLookup.sessionKeyEncrypted
    } yield AsymmetricEncrypted(sessionKeyEncrypted, encryptionAlgorithmSpec, fingerprint)

  def fromProto(
      encryptedViewMessageP: v30.EncryptedViewMessage
  ): ParsingResult[EncryptedViewMessage[ViewType]] = {
    val v30.EncryptedViewMessage(
      viewTreeP,
      encryptionSchemeP,
      signatureP,
      viewHashP,
      sessionKeyMapP,
      domainIdP,
      viewTypeP,
    ) =
      encryptedViewMessageP
    for {
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      viewEncryptionScheme <- SymmetricKeyScheme.fromProtoEnum(
        "encryptionScheme",
        encryptionSchemeP,
      )
      signature <- signatureP.traverse(Signature.fromProtoV30)
      viewTree = Encrypted.fromByteString[EncryptedView.CompressedView[viewType.View]](viewTreeP)
      encryptedView = EncryptedView(viewType)(viewTree)
      viewHash <- ViewHash.fromProtoPrimitive(viewHashP)
      sessionKeyRandomnessNE <- parseRequiredNonEmpty(
        deserializeSessionKeyEntry,
        "session key",
        sessionKeyMapP,
      )
      domainUid <- UniqueIdentifier.fromProtoPrimitive(domainIdP, "domainId")
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield new EncryptedViewMessage(
      signature,
      viewHash,
      sessionKeyRandomnessNE,
      encryptedView,
      DomainId(domainUid),
      viewEncryptionScheme,
    )(rpv)
  }

  def decryptRandomness[VT <: ViewType](
      allowedEncryptionSpecs: RequiredEncryptionSpecs,
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
      encrypted: EncryptedViewMessage[VT],
      participantId: ParticipantId,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, SecureRandomness] =
    for {
      /* We first need to check whether the target private encryption key exists and is active in the store; otherwise,
       * we cannot decrypt and should abort. This situation can occur
       * if an encryption key has been added to this participant's topology by another entity with the
       * correct rights to do so, but this participant does not have the corresponding private key in the store.
       */
      encryptionKeys <- EitherT
        .right(
          FutureUnlessShutdown.outcomeF(snapshot.ipsSnapshot.encryptionKeys(participantId))
        )
        .map(_.map(_.id).toSet)
      encryptedSessionKeyForParticipant <- encrypted.sessionKeys
        .find(e => encryptionKeys.contains(e.encryptedFor))
        .toRight(
          EncryptedViewMessageError.MissingParticipantKey(participantId)
        )
        .toEitherT[FutureUnlessShutdown]
      // TODO(#12911): throw an exception instead of a left for a missing private key in the store
      _ <- snapshot.crypto.cryptoPrivateStore
        .existsDecryptionKey(encryptedSessionKeyForParticipant.encryptedFor)
        .leftMap(err => EncryptedViewMessageError.PrivateKeyStoreVerificationError(err))
        .subflatMap {
          Either.cond(
            _,
            (),
            EncryptedViewMessageError.PrivateKeyStoreVerificationError(
              FailedToReadKey(
                encryptedSessionKeyForParticipant.encryptedFor,
                "matching private key does not exist",
              )
            ),
          )
        }
      _ <- checkEncryptionSpec(
        snapshot.crypto.cryptoPublicStore,
        encryptedSessionKeyForParticipant.encryptedFor,
        encryptedSessionKeyForParticipant.encryptionAlgorithmSpec,
        allowedEncryptionSpecs,
      )
        .leftMap(err =>
          EncryptedViewMessageError
            .SyncCryptoDecryptError(
              SyncCryptoDecryptionError(err)
            )
        )

      // we get the randomness for the session key from the message or by searching the cache. If this encrypted
      // randomness is in the cache this means that a previous view with the same recipients tree has been
      // received before.
      viewRandomness <-
        // we try to search for the cached session key randomness. If it does not exist
        // (or is disabled) we decrypt and store it
        // the result in the cache. There is no need to sync on this read-write operation because
        // there is no problem if the value gets re-written.
        sessionKeyStore
          .getSessionKeyRandomness(
            snapshot.crypto.privateCrypto,
            encrypted.viewEncryptionScheme.keySizeInBytes,
            encryptedSessionKeyForParticipant,
          )
          .leftMap[EncryptedViewMessageError](err =>
            SyncCryptoDecryptError(
              SyncCryptoDecryptionError(err)
            )
          )
    } yield viewRandomness

  private def eitherT[VT <: ViewType, B](value: Either[EncryptedViewMessageError, B])(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, B] =
    EitherT.fromEither[FutureUnlessShutdown](value)

  def computeRandomnessLength(pureCrypto: CryptoPureApi): Int =
    pureCrypto.defaultSymmetricKeyScheme.keySizeInBytes

  // This method is not defined as a member of EncryptedViewMessage because the covariant parameter VT conflicts
  // with the parameter deserialize.
  private def decryptWithRandomness[VT <: ViewType](
      snapshot: DomainSnapshotSyncCryptoApi,
      encrypted: EncryptedViewMessage[VT],
      viewRandomness: SecureRandomness,
  )(deserialize: ByteString => Either[DeserializationError, encrypted.encryptedView.viewType.View])(
      implicit ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, VT#View] = {

    val pureCrypto = snapshot.pureCrypto
    val randomnessLength = encrypted.viewEncryptionScheme.keySizeInBytes

    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        viewRandomness.unwrap.size == randomnessLength,
        (),
        EncryptedViewMessageError.WrongRandomnessLength(
          viewRandomness.unwrap.size,
          randomnessLength,
        ),
      )
      viewKey <- eitherT(
        pureCrypto
          .createSymmetricKey(viewRandomness, encrypted.viewEncryptionScheme)
          .leftMap(err =>
            EncryptedViewMessageError
              .SessionKeyCreationError(err)
          )
      )
      decrypted <- eitherT(
        EncryptedView
          .decrypt(pureCrypto, viewKey, encrypted.encryptedView)(deserialize)
          .leftMap(EncryptedViewMessageError.SymmetricDecryptError.apply)
      )
      _ <- eitherT(
        EitherUtil.condUnitE(
          decrypted.domainId == encrypted.domainId,
          EncryptedViewMessageError.WrongDomainIdInEncryptedViewMessage(
            encrypted.domainId,
            decrypted.domainId,
          ),
        )
      )
    } yield decrypted
  }

  def decryptFor[VT <: ViewType](
      staticDomainParameters: StaticDomainParameters,
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
      encrypted: EncryptedViewMessage[VT],
      participantId: ParticipantId,
      optViewRandomness: Option[SecureRandomness] = None,
  )(deserialize: ByteString => Either[DeserializationError, encrypted.encryptedView.viewType.View])(
      implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, VT#View] =
    // verify that the view symmetric encryption scheme is part of the required schemes
    if (
      !staticDomainParameters.requiredSymmetricKeySchemes
        .contains(encrypted.viewEncryptionScheme)
    ) {
      EitherT.leftT[FutureUnlessShutdown, VT#View](
        EncryptedViewMessageError.SymmetricDecryptError(
          InvariantViolation(
            s"The view symmetric encryption scheme ${encrypted.viewEncryptionScheme} is not " +
              s"part of the required schemes: ${staticDomainParameters.requiredSymmetricKeySchemes}"
          )
        )
      )
    } else {
      for {
        viewRandomness <- optViewRandomness.fold(
          decryptRandomness(
            staticDomainParameters.requiredEncryptionSpecs,
            snapshot,
            sessionKeyStore,
            encrypted,
            participantId,
          )
        )(r => EitherT.pure(r))
        decrypted <- decryptWithRandomness(snapshot, encrypted, viewRandomness)(
          deserialize
        )
      } yield decrypted
    }

  implicit val encryptedViewMessageCast
      : ProtocolMessageContentCast[EncryptedViewMessage[ViewType]] =
    ProtocolMessageContentCast.create[EncryptedViewMessage[ViewType]]("EncryptedViewMessage") {
      case evm: EncryptedViewMessage[_] => Some(evm)
      case _ => None
    }

  override def name: String = "EncryptedViewMessage"
}

sealed trait EncryptedViewMessageError extends Product with Serializable with PrettyPrinting {

  override protected def pretty: Pretty[EncryptedViewMessageError.this.type] = adHocPrettyInstance
}

object EncryptedViewMessageError {

  final case class SessionKeyCreationError(
      err: EncryptionKeyCreationError
  ) extends EncryptedViewMessageError

  final case class MissingParticipantKey(
      participantId: ParticipantId
  ) extends EncryptedViewMessageError

  final case class SyncCryptoDecryptError(
      syncCryptoError: SyncCryptoError
  ) extends EncryptedViewMessageError

  final case class SymmetricDecryptError(
      decryptError: DecryptionError
  ) extends EncryptedViewMessageError

  final case class WrongDomainIdInEncryptedViewMessage(
      declaredDomainId: DomainId,
      containedDomainId: DomainId,
  ) extends EncryptedViewMessageError

  final case class WrongRandomnessLength(
      length: Int,
      expectedLength: Int,
  ) extends EncryptedViewMessageError

  final case class PrivateKeyStoreVerificationError(
      privatekeyStoreError: CryptoPrivateStoreError
  ) extends EncryptedViewMessageError
}
