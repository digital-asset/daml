// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.CryptoDeserializationError
import com.digitalasset.canton.crypto.SyncCryptoError.SyncCryptoDecryptionError
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError.FailedToReadKey
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageError.{
  SessionKeyCreationError,
  SyncCryptoDecryptError,
  WrongRandomnessLength,
}
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageV1.RecipientsInfo
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseRequiredNonEmpty}
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

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
  override def canEqual(that: Any): Boolean = that.isInstanceOf[EncryptedView[_]]
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.IsInstanceOf",
      "org.wartremover.warts.Null",
    )
  )
  override def equals(that: Any): Boolean = {
    if (this eq that.asInstanceOf[Object]) true
    else if (!that.isInstanceOf[EncryptedView[_]]) false
    else {
      val other = that.asInstanceOf[EncryptedView[ViewType]]
      val thisViewTree = this.viewTree
      if (thisViewTree eq null) other.viewTree eq null else thisViewTree == other.viewTree
    }
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
      version: ProtocolVersion,
  )(aViewTree: aViewType.View): Either[EncryptionError, EncryptedView[VT]] =
    encryptionOps
      .encryptWith(CompressedView(aViewTree), viewKey, version)
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
  final case class CompressedView[+V <: HasVersionedToByteString] private (value: V)
      extends HasVersionedToByteString {
    override def toByteString(version: ProtocolVersion): ByteString =
      ByteStringUtil.compressGzip(value.toByteString(version))
  }

  object CompressedView {
    private[EncryptedView] def apply[V <: HasVersionedToByteString](value: V): CompressedView[V] =
      new CompressedView(value)

    private[EncryptedView] def fromByteString[V <: HasVersionedToByteString](
        deserialize: ByteString => Either[DeserializationError, V]
    )(bytes: ByteString): Either[DeserializationError, CompressedView[V]] =
      // TODO(i10428) Make sure that this view does not explode into an arbitrarily large object
      ByteStringUtil
        .decompressGzip(bytes, maxBytesLimit = None)
        .flatMap(deserialize)
        .map(CompressedView(_))
  }

}

/** An encrypted view message.
  *
  * See [[https://engineering.da-int.net/docs/platform-architecture-handbook/arch/canton/tx-data-structures.html#transaction-hashes-and-views]]
  */
sealed trait EncryptedViewMessage[+VT <: ViewType] extends UnsignedProtocolMessage {

  protected[messages] def recipientsInfo: Option[RecipientsInfo]

  /** The symmetric encryption scheme that was used to encrypt the view */
  protected def viewEncryptionScheme: SymmetricKeyScheme

  protected def updateView[VT2 <: ViewType](newView: EncryptedView[VT2]): EncryptedViewMessage[VT2]

  // We can't include it into the SubmitterMetadata, because that would create a cycle dependency:
  // - The signature depends on the transaction id.
  // - The transaction id depends on the submitter metadata.
  /** An optional submitter participant's signature. */
  def submitterParticipantSignature: Option[Signature]

  /** Transaction view hash in plain text - included such that the recipient can prove to a 3rd party
    * that it has correctly decrypted the `viewTree`
    */
  def viewHash: ViewHash

  val encryptedView: EncryptedView[VT]

  def viewType: VT = encryptedView.viewType

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], VT2 <: ViewType](
      f: EncryptedView[VT] => F[EncryptedView[VT2]]
  )(implicit F: Functor[F]): F[EncryptedViewMessage[VT2]] = {
    F.map(f(encryptedView)) { newEncryptedView =>
      if (newEncryptedView eq encryptedView) this.asInstanceOf[EncryptedViewMessage[VT2]]
      else updateView(newEncryptedView)
    }
  }

  override def pretty: Pretty[EncryptedViewMessage.this.type] = prettyOfClass(
    param("view hash", _.viewHash),
    param("view type", _.viewType),
    param("size", _.encryptedView.sizeHint),
  )

  def toByteString: ByteString

  @transient override protected lazy val companionObj: EncryptedViewMessage.type =
    EncryptedViewMessage
}

final case class EncryptedViewMessageV0[+VT <: ViewType](
    submitterParticipantSignature: Option[Signature],
    viewHash: ViewHash,
    randomnessMap: Map[ParticipantId, Encrypted[SecureRandomness]],
    encryptedView: EncryptedView[VT],
    override val domainId: DomainId,
) extends EncryptedViewMessage[VT]
    with ProtocolMessageV0 {

  protected[messages] override def recipientsInfo: Option[RecipientsInfo] = Some(
    RecipientsInfo(randomnessMap.keySet)
  )

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[EncryptedViewMessage.type] =
    EncryptedViewMessage.protocolVersionRepresentativeFor(ProtoVersion(0))

  def toProtoV0: v0.EncryptedViewMessage =
    v0.EncryptedViewMessage(
      viewTree = encryptedView.viewTree.ciphertext,
      submitterParticipantSignature = submitterParticipantSignature.map(_.toProtoV0),
      viewHash = viewHash.toProtoPrimitive,
      randomness = randomnessMap.map(EncryptedViewMessageV0.serializeRandomnessEntry).toSeq,
      domainId = domainId.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
    )

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(v0.EnvelopeContent.SomeEnvelopeContent.EncryptedViewMessage(toProtoV0))

  override def viewEncryptionScheme: SymmetricKeyScheme = SymmetricKeyScheme.Aes128Gcm

  override protected def updateView[VT2 <: ViewType](
      newView: EncryptedView[VT2]
  ): EncryptedViewMessage[VT2] = copy(encryptedView = newView)

  override def toByteString: ByteString = toProtoV0.toByteString
}

final case class EncryptedViewMessageV1[+VT <: ViewType](
    submitterParticipantSignature: Option[Signature],
    viewHash: ViewHash,
    randomness: Seq[AsymmetricEncrypted[SecureRandomness]],
    encryptedView: EncryptedView[VT],
    override val domainId: DomainId,
    viewEncryptionScheme: SymmetricKeyScheme,
)(
    val recipientsInfo: Option[RecipientsInfo]
) extends EncryptedViewMessage[VT]
    with ProtocolMessageV1
    with ProtocolMessageV2 {

  def copy[A <: ViewType](
      submitterParticipantSignature: Option[Signature] = this.submitterParticipantSignature,
      viewHash: ViewHash = this.viewHash,
      randomness: Seq[AsymmetricEncrypted[SecureRandomness]] = this.randomness,
      encryptedView: EncryptedView[A] = this.encryptedView,
      domainId: DomainId = this.domainId,
      viewEncryptionScheme: SymmetricKeyScheme = this.viewEncryptionScheme,
  ): EncryptedViewMessageV1[A] = EncryptedViewMessageV1(
    submitterParticipantSignature,
    viewHash,
    randomness,
    encryptedView,
    domainId,
    viewEncryptionScheme,
  )(recipientsInfo)

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[EncryptedViewMessage.type] =
    EncryptedViewMessage.protocolVersionRepresentativeFor(ProtoVersion(1))

  def toProtoV1: v1.EncryptedViewMessage = v1.EncryptedViewMessage(
    viewTree = encryptedView.viewTree.ciphertext,
    encryptionScheme = viewEncryptionScheme.toProtoEnum,
    submitterParticipantSignature = submitterParticipantSignature.map(_.toProtoV0),
    viewHash = viewHash.toProtoPrimitive,
    randomness = randomness.map(EncryptedViewMessageV1.serializeRandomnessEntry),
    domainId = domainId.toProtoPrimitive,
    viewType = viewType.toProtoEnum,
  )

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(v1.EnvelopeContent.SomeEnvelopeContent.EncryptedViewMessage(toProtoV1))

  override def toProtoEnvelopeContentV2: v2.EnvelopeContent =
    v2.EnvelopeContent(v2.EnvelopeContent.SomeEnvelopeContent.EncryptedViewMessage(toProtoV1))

  override protected def updateView[VT2 <: ViewType](
      newView: EncryptedView[VT2]
  ): EncryptedViewMessage[VT2] =
    copy(encryptedView = newView)

  override def toByteString: ByteString = toProtoV1.toByteString
}

/** The view message encrypted with symmetric key that is derived from the view's randomness.
  *
  * @param randomness the view's randomness symmetrically encrypted with a session key.
  * @param sessionKey a sequence of encrypted random values to each recipient of the view.
  *                   These values are encrypted and are used to derive the symmetric session key.
  *                   Instead of sending a <SymmetricKey>, which could cause formatting issues
  *                   (e.g. different participants with different providers and, therefore, different key formats),
  *                   we send an encrypted <SecureRandomness>.
  */
final case class EncryptedViewMessageV2[+VT <: ViewType](
    submitterParticipantSignature: Option[Signature],
    viewHash: ViewHash,
    randomness: Encrypted[SecureRandomness],
    sessionKey: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]],
    encryptedView: EncryptedView[VT],
    override val domainId: DomainId,
    viewEncryptionScheme: SymmetricKeyScheme,
)(
    val recipientsInfo: Option[RecipientsInfo]
) extends EncryptedViewMessage[VT]
    with ProtocolMessageV3 {

  def copy[A <: ViewType](
      submitterParticipantSignature: Option[Signature] = this.submitterParticipantSignature,
      viewHash: ViewHash = this.viewHash,
      randomness: Encrypted[SecureRandomness] = this.randomness,
      sessionKeyRandomness: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]] = this.sessionKey,
      encryptedView: EncryptedView[A] = this.encryptedView,
      domainId: DomainId = this.domainId,
      viewEncryptionScheme: SymmetricKeyScheme = this.viewEncryptionScheme,
  ): EncryptedViewMessageV2[A] = EncryptedViewMessageV2(
    submitterParticipantSignature,
    viewHash,
    randomness,
    sessionKeyRandomness,
    encryptedView,
    domainId,
    viewEncryptionScheme,
  )(recipientsInfo)

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[EncryptedViewMessage.type] =
    EncryptedViewMessage.protocolVersionRepresentativeFor(ProtoVersion(2))

  private def toProtoV2: v2.EncryptedViewMessage = v2.EncryptedViewMessage(
    viewTree = encryptedView.viewTree.ciphertext,
    encryptionScheme = viewEncryptionScheme.toProtoEnum,
    submitterParticipantSignature = submitterParticipantSignature.map(_.toProtoV0),
    viewHash = viewHash.toProtoPrimitive,
    randomness = randomness.ciphertext,
    sessionKeyRandomness = sessionKey.map(EncryptedViewMessageV2.serializeSessionKeyEntry),
    domainId = domainId.toProtoPrimitive,
    viewType = viewType.toProtoEnum,
  )

  override def toProtoEnvelopeContentV3: v3.EnvelopeContent =
    v3.EnvelopeContent(v3.EnvelopeContent.SomeEnvelopeContent.EncryptedViewMessage(toProtoV2))

  override protected def updateView[VT2 <: ViewType](
      newView: EncryptedView[VT2]
  ): EncryptedViewMessage[VT2] = copy(encryptedView = newView)

  override def toByteString: ByteString = toProtoV2.toByteString
}

object EncryptedViewMessageV0 {

  private def serializeRandomnessEntry(
      entry: (ParticipantId, Encrypted[SecureRandomness])
  ): v0.ParticipantRandomnessLookup = {
    val (participant, encryptedRandomness) = entry
    v0.ParticipantRandomnessLookup(
      participant = participant.toProtoPrimitive,
      randomness = encryptedRandomness.ciphertext,
    )
  }

  private def deserializeRandomnessEntry(
      randomnessLookup: v0.ParticipantRandomnessLookup
  ): ParsingResult[(ParticipantId, Encrypted[SecureRandomness])] =
    for {
      participantId <- ParticipantId.fromProtoPrimitive(randomnessLookup.participant, "participant")
      encryptedKey <- Encrypted
        .fromByteString[SecureRandomness](randomnessLookup.randomness)
        .leftMap(CryptoDeserializationError)
    } yield (participantId, encryptedKey)

  def fromProto(
      encryptedViewMessageP: v0.EncryptedViewMessage
  ): ParsingResult[EncryptedViewMessageV0[ViewType]] = {
    val v0.EncryptedViewMessage(
      viewTreeP,
      signatureP,
      viewHashP,
      randomnessMapP,
      domainIdP,
      viewTypeP,
    ) =
      encryptedViewMessageP
    for {
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      signature <- signatureP.traverse(Signature.fromProtoV0)
      viewTree <- Encrypted
        .fromByteString[EncryptedView.CompressedView[viewType.View]](viewTreeP)
        .leftMap(CryptoDeserializationError)
      encryptedView = EncryptedView(viewType)(viewTree)
      viewHash <- ViewHash.fromProtoPrimitive(viewHashP)
      randomnessList <- randomnessMapP.traverse(deserializeRandomnessEntry)
      randomnessMap = randomnessList.toMap
      domainUid <- UniqueIdentifier.fromProtoPrimitive(domainIdP, "domainId")
    } yield new EncryptedViewMessageV0(
      signature,
      viewHash,
      randomnessMap,
      encryptedView,
      DomainId(domainUid),
    )
  }

  def decryptRandomness[VT <: ViewType](
      snapshot: DomainSnapshotSyncCryptoApi,
      encrypted: EncryptedViewMessageV0[VT],
      participantId: ParticipantId,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, EncryptedViewMessageError, SecureRandomness] = {
    val randomnessLength = EncryptedViewMessage.computeRandomnessLength(snapshot.pureCrypto)

    for {
      encryptedRandomness <-
        encrypted.randomnessMap
          .get(participantId)
          .toRight(
            EncryptedViewMessageError.MissingParticipantKey(participantId)
          )
          .toEitherT[Future]
      viewRandomness <- snapshot
        .decrypt(encryptedRandomness)(SecureRandomness.fromByteString(randomnessLength))
        .leftMap[EncryptedViewMessageError](
          EncryptedViewMessageError.SyncCryptoDecryptError(_)
        )
    } yield viewRandomness
  }

}

object EncryptedViewMessageV1 {

  final case class RecipientsInfo(
      informeeParticipants: Set[ParticipantId]
  )

  private def serializeRandomnessEntry(
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness]
  ): v1.ParticipantRandomnessLookup = {
    v1.ParticipantRandomnessLookup(
      randomness = encryptedRandomness.ciphertext,
      fingerprint = encryptedRandomness.encryptedFor.toProtoPrimitive,
    )
  }

  private def deserializeRandomnessEntry(
      randomnessLookup: v1.ParticipantRandomnessLookup
  ): ParsingResult[AsymmetricEncrypted[SecureRandomness]] =
    for {
      fingerprint <- Fingerprint.fromProtoPrimitive(randomnessLookup.fingerprint)
      encryptedRandomness = randomnessLookup.randomness
    } yield AsymmetricEncrypted(encryptedRandomness, fingerprint)

  def fromProto(
      encryptedViewMessageP: v1.EncryptedViewMessage
  ): ParsingResult[EncryptedViewMessageV1[ViewType]] = {
    val v1.EncryptedViewMessage(
      viewTreeP,
      encryptionSchemeP,
      signatureP,
      viewHashP,
      randomnessMapP,
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
      signature <- signatureP.traverse(Signature.fromProtoV0)
      viewTree <- Encrypted
        .fromByteString[EncryptedView.CompressedView[viewType.View]](viewTreeP)
        .leftMap(CryptoDeserializationError)
      encryptedView = EncryptedView(viewType)(viewTree)
      viewHash <- ViewHash.fromProtoPrimitive(viewHashP)
      randomness <- randomnessMapP.traverse(deserializeRandomnessEntry)
      domainUid <- UniqueIdentifier.fromProtoPrimitive(domainIdP, "domainId")
    } yield new EncryptedViewMessageV1(
      signature,
      viewHash,
      randomness,
      encryptedView,
      DomainId(domainUid),
      viewEncryptionScheme,
    )(None)
  }

  def decryptRandomness[VT <: ViewType](
      snapshot: DomainSnapshotSyncCryptoApi,
      encrypted: EncryptedViewMessageV1[VT],
      participantId: ParticipantId,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, EncryptedViewMessageError, SecureRandomness] =
    for {
      encryptionKeys <- EitherT
        .right(snapshot.ipsSnapshot.encryptionKeys(participantId))
        .map(_.map(_.id).toSet)
      encryptedRandomnessForParticipant <- encrypted.randomness
        .find(e => encryptionKeys.contains(e.encryptedFor))
        .toRight(
          EncryptedViewMessageError.MissingParticipantKey(participantId)
        )
        .toEitherT[Future]
      viewRandomness <- snapshot
        .decrypt(encryptedRandomnessForParticipant)(
          SecureRandomness.fromByteString(
            EncryptedViewMessage.computeRandomnessLength(snapshot.pureCrypto)
          )
        )
        .leftMap[EncryptedViewMessageError](
          EncryptedViewMessageError.SyncCryptoDecryptError(_)
        )
    } yield viewRandomness

}

object EncryptedViewMessageV2 {

  private def serializeSessionKeyEntry(
      encryptedSessionKey: AsymmetricEncrypted[SecureRandomness]
  ): v2.SessionKeyLookup = {
    v2.SessionKeyLookup(
      sessionKeyRandomness = encryptedSessionKey.ciphertext,
      fingerprint = encryptedSessionKey.encryptedFor.toProtoPrimitive,
    )
  }

  private def deserializeSessionKeyEntry(
      sessionKeyLookup: v2.SessionKeyLookup
  ): ParsingResult[AsymmetricEncrypted[SecureRandomness]] =
    for {
      fingerprint <- Fingerprint.fromProtoPrimitive(sessionKeyLookup.fingerprint)
      sessionKeyRandomness = sessionKeyLookup.sessionKeyRandomness
    } yield AsymmetricEncrypted(sessionKeyRandomness, fingerprint)

  def fromProto(
      encryptedViewMessageP: v2.EncryptedViewMessage
  ): ParsingResult[EncryptedViewMessageV2[ViewType]] = {
    val v2.EncryptedViewMessage(
      viewTreeP,
      encryptionSchemeP,
      signatureP,
      viewHashP,
      randomnessP,
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
      signature <- signatureP.traverse(Signature.fromProtoV0)
      viewTree <- Encrypted
        .fromByteString[EncryptedView.CompressedView[viewType.View]](viewTreeP)
        .leftMap(CryptoDeserializationError)
      encryptedView = EncryptedView(viewType)(viewTree)
      viewHash <- ViewHash.fromProtoPrimitive(viewHashP)
      randomness <- Encrypted
        .fromByteString[SecureRandomness](randomnessP)
        .leftMap(CryptoDeserializationError)
      sessionKeyRandomnessNE <- parseRequiredNonEmpty(
        deserializeSessionKeyEntry,
        "session key",
        sessionKeyMapP,
      )
      domainUid <- UniqueIdentifier.fromProtoPrimitive(domainIdP, "domainId")
    } yield new EncryptedViewMessageV2(
      signature,
      viewHash,
      randomness,
      sessionKeyRandomnessNE,
      encryptedView,
      DomainId(domainUid),
      viewEncryptionScheme,
    )(None)
  }

  def decryptRandomness[VT <: ViewType](
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
      encrypted: EncryptedViewMessageV2[VT],
      participantId: ParticipantId,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, EncryptedViewMessageError, SecureRandomness] = {
    val pureCrypto = snapshot.pureCrypto

    val randomnessLength = EncryptedViewMessage.computeRandomnessLength(pureCrypto)

    def decryptViewRandomness(
        sessionKeyRandomness: SecureRandomness
    ): EitherT[Future, EncryptedViewMessageError, SecureRandomness] =
      for {
        // derive symmetric key from randomness
        sessionKey <- pureCrypto
          .createSymmetricKey(sessionKeyRandomness, encrypted.viewEncryptionScheme)
          .leftMap[EncryptedViewMessageError](SessionKeyCreationError(_))
          .toEitherT[Future]
        randomness <- pureCrypto
          .decryptWith(encrypted.randomness, sessionKey)(
            SecureRandomness.fromByteString(randomnessLength)
          )
          .leftMap[EncryptedViewMessageError](
            EncryptedViewMessageError.SymmetricDecryptError(_)
          )
          .toEitherT[Future]
      } yield randomness

    encrypted.sessionKey
      .collectFirst {
        case AsymmetricEncrypted(ciphertext, encryptedFor)
            // if we're using no encryption, it means we're using group addressing
            // which currently does not support encryption of the randomness
            if encryptedFor == AsymmetricEncrypted.noEncryptionFingerprint =>
          SecureRandomness
            .fromByteString(randomnessLength)(encrypted.randomness.ciphertext)
            .leftMap[EncryptedViewMessageError](_ =>
              WrongRandomnessLength(ciphertext.size(), randomnessLength)
            )
            .toEitherT[Future]
      }
      .getOrElse {
        for {
          /* We first need to check whether the target private encryption key exists and is active in the store; otherwise,
           * we cannot decrypt and should abort. This situation can occur
           * if an encryption key has been added to this participant's topology by another entity with the
           * correct rights to do so, but this participant does not have the corresponding private key in the store.
           */
          encryptionKeys <- EitherT
            .right(snapshot.ipsSnapshot.encryptionKeys(participantId))
            .map(_.map(_.id).toSet)
          encryptedSessionKeyForParticipant <- encrypted.sessionKey
            .find(e => encryptionKeys.contains(e.encryptedFor))
            .toRight(
              EncryptedViewMessageError.MissingParticipantKey(participantId)
            )
            .toEitherT[Future]
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

          // we get the randomness for the session key from the message or by searching the cache,
          // which means that a previous view with the same recipients has been received before.
          skRandom <-
            // we try to search for the cached session key randomness. If it does not exist
            // (or is disabled) we decrypt and store it
            // the result in the cache. There is no need to sync on this read-write operation because
            // there is not problem if the value gets re-written.
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
          viewRandomness <- decryptViewRandomness(skRandom)
        } yield viewRandomness
      }
  }

}

object EncryptedViewMessage extends HasProtocolVersionedCompanion[EncryptedViewMessage[_]] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.EncryptedViewMessage)(
      supportedProtoVersion(_)(EncryptedViewMessageV0.fromProto),
      _.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.EncryptedViewMessage)(
      supportedProtoVersion(_)(EncryptedViewMessageV1.fromProto),
      _.toByteString,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v6)(v2.EncryptedViewMessage)(
      supportedProtoVersion(_)(EncryptedViewMessageV2.fromProto),
      _.toByteString,
    ),
  )

  private def eitherT[VT <: ViewType, B](value: Either[EncryptedViewMessageError, B])(implicit
      ec: ExecutionContext
  ): EitherT[Future, EncryptedViewMessageError, B] =
    EitherT.fromEither[Future](value)

  def computeRandomnessLength(pureCrypto: CryptoPureApi): Int =
    pureCrypto.defaultHashAlgorithm.length.toInt

  // This method is not defined as a member of EncryptedViewMessage because the covariant parameter VT conflicts
  // with the parameter deserialize.
  def decryptWithRandomness[VT <: ViewType](
      snapshot: DomainSnapshotSyncCryptoApi,
      encrypted: EncryptedViewMessage[VT],
      viewRandomness: SecureRandomness,
      protocolVersion: ProtocolVersion,
  )(deserialize: ByteString => Either[DeserializationError, encrypted.encryptedView.viewType.View])(
      implicit ec: ExecutionContext
  ): EitherT[Future, EncryptedViewMessageError, VT#View] = {

    val pureCrypto = snapshot.pureCrypto
    val viewKeyLength = encrypted.viewEncryptionScheme.keySizeInBytes
    val randomnessLength = computeRandomnessLength(snapshot.pureCrypto)

    for {
      _ <- EitherT.cond[Future](
        viewRandomness.unwrap.size == randomnessLength,
        (),
        EncryptedViewMessageError.WrongRandomnessLength(
          viewRandomness.unwrap.size,
          randomnessLength,
        ),
      )
      viewKeyRandomness <-
        eitherT(
          pureCrypto
            .computeHkdf(viewRandomness.unwrap, viewKeyLength, HkdfInfo.ViewKey)
            .leftMap(EncryptedViewMessageError.HkdfExpansionError(_))
        )
      viewKey <- eitherT(
        pureCrypto
          .createSymmetricKey(viewKeyRandomness)
          .leftMap(err =>
            EncryptedViewMessageError
              .SymmetricDecryptError(DecryptionError.InvalidSymmetricKey(err.toString))
          )
      )
      decrypted <- eitherT(
        EncryptedView
          .decrypt(pureCrypto, viewKey, encrypted.encryptedView)(deserialize)
          .leftMap(EncryptedViewMessageError.SymmetricDecryptError(_))
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

  def decryptRandomness[VT <: ViewType](
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
      encrypted: EncryptedViewMessage[VT],
      participantId: ParticipantId,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, EncryptedViewMessageError, SecureRandomness] =
    encrypted match {
      case encryptedV0: EncryptedViewMessageV0[VT] =>
        EncryptedViewMessageV0.decryptRandomness(snapshot, encryptedV0, participantId)
      case encryptedV1: EncryptedViewMessageV1[VT] =>
        EncryptedViewMessageV1.decryptRandomness(snapshot, encryptedV1, participantId)
      case encryptedV2: EncryptedViewMessageV2[VT] =>
        EncryptedViewMessageV2.decryptRandomness(
          snapshot,
          sessionKeyStore,
          encryptedV2,
          participantId,
        )
    }

  def decryptFor[VT <: ViewType](
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
      encrypted: EncryptedViewMessage[VT],
      participantId: ParticipantId,
      protocolVersion: ProtocolVersion,
      optViewRandomness: Option[SecureRandomness] = None,
  )(deserialize: ByteString => Either[DeserializationError, encrypted.encryptedView.viewType.View])(
      implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, EncryptedViewMessageError, VT#View] = {

    val decryptedRandomness =
      decryptRandomness(snapshot, sessionKeyStore, encrypted, participantId)

    for {
      viewRandomness <- optViewRandomness.fold(
        decryptedRandomness
      )(r => EitherT.pure(r))
      decrypted <- decryptWithRandomness(snapshot, encrypted, viewRandomness, protocolVersion)(
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

  override def pretty: Pretty[EncryptedViewMessageError.this.type] = adHocPrettyInstance
}

object EncryptedViewMessageError {

  final case class SessionKeyCreationError(
      keyCreationError: EncryptionKeyCreationError
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

  final case class HkdfExpansionError(
      cause: HkdfError
  ) extends EncryptedViewMessageError

  final case class WrongRandomnessLength(
      length: Int,
      expectedLength: Int,
  ) extends EncryptedViewMessageError

  final case class PrivateKeyStoreVerificationError(
      privatekeyStoreError: CryptoPrivateStoreError
  ) extends EncryptedViewMessageError
}
