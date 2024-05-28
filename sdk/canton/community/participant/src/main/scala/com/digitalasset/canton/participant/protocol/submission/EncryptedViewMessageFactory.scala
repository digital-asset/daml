// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.RecipientsInfo
import com.digitalasset.canton.protocol.messages.{EncryptedView, EncryptedViewMessage}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.store.SessionKeyStore.RecipientGroup
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

object EncryptedViewMessageFactory {

  def create[VT <: ViewType](viewType: VT)(
      viewTree: viewType.View,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
      protocolVersion: ProtocolVersion,
      optRandomness: Option[SecureRandomness] = None,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageCreationError, EncryptedViewMessage[VT]] = {

    val cryptoPureApi = cryptoSnapshot.pureCrypto

    val viewEncryptionScheme = cryptoPureApi.defaultSymmetricKeyScheme
    val viewKeyLength = viewEncryptionScheme.keySizeInBytes
    val randomnessLength = EncryptedViewMessage.computeRandomnessLength(cryptoPureApi)
    val randomness: SecureRandomness =
      optRandomness.getOrElse(cryptoPureApi.generateSecureRandomness(randomnessLength))

    val informeeParties = viewTree.informees.toList

    def eitherT[B](
        value: Either[EncryptedViewMessageCreationError, B]
    ): EitherT[Future, EncryptedViewMessageCreationError, B] =
      EitherT.fromEither[Future](value)

    def eitherTUS[B](
        value: Either[EncryptedViewMessageCreationError, B]
    ): EitherT[FutureUnlessShutdown, EncryptedViewMessageCreationError, B] =
      EitherT.fromEither[FutureUnlessShutdown](value)

    def getRecipientInfo: EitherT[Future, UnableToDetermineParticipant, RecipientsInfo] = {
      for {
        informeeParticipants <- cryptoSnapshot.ipsSnapshot
          .activeParticipantsOfAll(informeeParties)
          .leftMap(UnableToDetermineParticipant(_, cryptoSnapshot.domainId))
        partiesWithGroupAddressing <- EitherT.right(
          cryptoSnapshot.ipsSnapshot.partiesWithGroupAddressing(informeeParties)
        )
      } yield RecipientsInfo(
        informeeParticipants = informeeParticipants,
        doNotEncrypt = partiesWithGroupAddressing.nonEmpty,
      )
    }

    def generateAndEncryptSessionKeyRandomness(
        recipients: NonEmpty[Set[ParticipantId]]
    ): EitherT[
      Future,
      EncryptedViewMessageCreationError,
      (SymmetricKey, Seq[AsymmetricEncrypted[SecureRandomness]]),
    ] =
      for {
        keyRandomness <- eitherT(
          cryptoPureApi
            .computeHkdf(
              cryptoPureApi.generateSecureRandomness(randomnessLength).unwrap,
              viewKeyLength,
              HkdfInfo.SessionKey,
            )
            .leftMap(FailedToExpandKey)
        )
        key <- eitherT(
          cryptoPureApi
            .createSymmetricKey(keyRandomness, viewEncryptionScheme)
            .leftMap(FailedToCreateEncryptionKey)
        )
        // generates the session key map, which contains the session key randomness encrypted for all recipients
        keyMap <- createDataMap(
          recipients.forgetNE.to(LazyList),
          keyRandomness,
          cryptoSnapshot,
          protocolVersion,
        ).map(_.values.toSeq)
        _ = sessionKeyStore.saveSessionKeyInfo(
          RecipientGroup(recipients, viewEncryptionScheme),
          SessionKeyInfo(keyRandomness, keyMap),
        )
      } yield (key, keyMap)

    // Either we generate and encrypt the session key (randomness) or we retrieve it from the cache
    def getSessionKey(
        recipientsInfo: RecipientsInfo
    ): EitherT[
      Future,
      EncryptedViewMessageCreationError,
      (SymmetricKey, Seq[AsymmetricEncrypted[SecureRandomness]]),
    ] =
      for {
        recipients <- eitherT(
          NonEmpty
            .from(recipientsInfo.informeeParticipants)
            .toRight(UnableToDetermineRecipients("The list of recipients is empty"))
        )
        sessionKeyAndMap <-
          // check that we already have a session key for a similar transaction that we can reuse
          sessionKeyStore.getSessionKeyInfoIfPresent(
            RecipientGroup(recipients, viewEncryptionScheme)
          ) match {
            case Some(sessionKeyInfo) =>
              // we need to check that the public keys match if they have been changed in the meantime. If they did
              // we are revoking the session key because that public key could have been compromised
              val allPubKeysIdsInMessage = sessionKeyInfo.encryptedSessionKeys.map(_.encryptedFor)
              for {
                // check that that all recipients are represented in the message, in other words,
                // that at least one of their active public keys is present in the sequence `encryptedSessionKeys`
                checkActiveParticipantKeys <-
                  EitherT
                    .right[EncryptedViewMessageCreationError](
                      cryptoSnapshot.ipsSnapshot
                        .encryptionKeys(recipients.forgetNE.toSeq)
                        .map { memberToKeysMap =>
                          recipients.map(recipient =>
                            memberToKeysMap
                              .getOrElse(recipient, Seq.empty)
                              .exists(key => allPubKeysIdsInMessage.contains(key.id))
                          )
                        }
                    )
                    .map(_.forall(_ == true))
                // all public keys used to encrypt the session key must be present and active for each recipient
                sessionKeyData <-
                  if (checkActiveParticipantKeys)
                    eitherT(
                      cryptoPureApi
                        .createSymmetricKey(
                          sessionKeyInfo.sessionKeyRandomness,
                          viewEncryptionScheme,
                        )
                        .leftMap(FailedToCreateEncryptionKey)
                        .map((_, sessionKeyInfo.encryptedSessionKeys))
                    )
                  else
                    generateAndEncryptSessionKeyRandomness(recipients)
              } yield sessionKeyData
            case None =>
              // if not in cache we need to generate a new session key for this view and save it
              generateAndEncryptSessionKeyRandomness(recipients)
          }
      } yield sessionKeyAndMap

    def createEncryptedViewMessage(
        recipientsInfo: RecipientsInfo,
        signature: Option[Signature],
        encryptedView: EncryptedView[VT],
    ): EitherT[Future, EncryptedViewMessageCreationError, EncryptedViewMessage[VT]] =
      (if (!recipientsInfo.doNotEncrypt) {
         for {
           sessionKeyAndRandomnessMap <- getSessionKey(recipientsInfo)
           (sessionKey, sessionKeyRandomnessMap) = sessionKeyAndRandomnessMap
           sessionKeyRandomnessMapNE <- EitherT.fromEither[Future](
             NonEmpty
               .from(sessionKeyRandomnessMap)
               .toRight(
                 UnableToDetermineSessionKeyRandomness(
                   "The session key randomness map is empty"
                 )
               )
           )
           encryptedSessionKeyInfo <- encryptRandomnessWithSessionKey(sessionKey).map(
             encryptedRandomness => (encryptedRandomness, sessionKeyRandomnessMapNE)
           )
         } yield encryptedSessionKeyInfo
       } else {
         val encryptedRandomness = Encrypted.fromByteString[SecureRandomness](randomness.unwrap)
         eitherT(
           Right(
             (
               encryptedRandomness,
               NonEmpty(
                 Seq,
                 AsymmetricEncrypted[SecureRandomness](
                   ByteString.EMPTY,
                   AsymmetricEncrypted.noEncryptionFingerprint,
                 ),
               ),
             )
           )
         )
       }).map { case (randomnessV2, sessionKeyMap) =>
        EncryptedViewMessage[VT](
          signature,
          viewTree.viewHash,
          randomnessV2,
          sessionKeyMap,
          encryptedView,
          viewTree.domainId,
          viewEncryptionScheme,
          protocolVersion,
        )
      }

    def encryptRandomnessWithSessionKey(
        sessionKey: SymmetricKey
    ): EitherT[Future, EncryptedViewMessageCreationError, Encrypted[
      SecureRandomness
    ]] =
      eitherT(
        cryptoPureApi
          .encryptWith(randomness, sessionKey, protocolVersion)
          .leftMap(FailedToEncryptRandomness)
      )

    for {
      symmetricViewKeyRandomness <- eitherTUS(
        cryptoPureApi
          .computeHkdf(randomness.unwrap, viewKeyLength, HkdfInfo.ViewKey)
          .leftMap(FailedToExpandKey)
      )
      symmetricViewKey <- eitherTUS(
        cryptoPureApi
          .createSymmetricKey(symmetricViewKeyRandomness, viewEncryptionScheme)
          .leftMap(FailedToCreateEncryptionKey)
      )
      recipientsInfo <- getRecipientInfo.mapK(FutureUnlessShutdown.outcomeK)
      signature <- viewTree.toBeSigned
        .parTraverse(rootHash =>
          cryptoSnapshot.sign(rootHash.unwrap).leftMap(FailedToSignViewMessage)
        )
      encryptedView <- eitherTUS(
        EncryptedView
          .compressed[VT](cryptoPureApi, symmetricViewKey, viewType)(viewTree)
          .leftMap(FailedToEncryptViewMessage)
      )
      message <- createEncryptedViewMessage(recipientsInfo, signature, encryptedView).mapK(
        FutureUnlessShutdown.outcomeK
      )
    } yield message
  }

  private def createDataMap[M <: HasVersionedToByteString](
      participants: LazyList[ParticipantId],
      data: M,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      version: ProtocolVersion,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, EncryptedViewMessageCreationError, Map[
    ParticipantId,
    AsymmetricEncrypted[M],
  ]] =
    cryptoSnapshot
      .encryptFor(data, participants, version)
      .leftMap { case (member, error) =>
        UnableToDetermineKey(
          member,
          error,
          cryptoSnapshot.domainId,
        ): EncryptedViewMessageCreationError
      }
      .map(_.toMap)

  sealed trait EncryptedViewMessageCreationError
      extends Product
      with Serializable
      with PrettyPrinting

  /** Indicates that we could not determine the recipients of the underlying view
    */
  final case class UnableToDetermineRecipients(cause: String)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[UnableToDetermineRecipients] = prettyOfClass(
      param("cause", _.cause.unquoted)
    )
  }

  /** Indicates that the participant hosting one or more informees could not be determined.
    */
  final case class UnableToDetermineParticipant(party: Set[LfPartyId], domain: DomainId)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[UnableToDetermineParticipant] =
      prettyOfClass(unnamedParam(_.party), unnamedParam(_.domain))
  }

  /** Indicates that the public key of an informee participant could not be determined.
    */
  final case class UnableToDetermineKey(
      participant: ParticipantId,
      cause: SyncCryptoError,
      domain: DomainId,
  ) extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[UnableToDetermineKey] = prettyOfClass(
      param("participant", _.participant),
      param("cause", _.cause),
    )
  }

  final case class FailedToGenerateEncryptionKey(cause: EncryptionKeyGenerationError)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[FailedToGenerateEncryptionKey] = prettyOfClass(
      unnamedParam(_.cause)
    )
  }

  final case class FailedToCreateEncryptionKey(cause: EncryptionKeyCreationError)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[FailedToCreateEncryptionKey] = prettyOfClass(
      unnamedParam(_.cause)
    )
  }

  final case class FailedToExpandKey(cause: HkdfError) extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[FailedToExpandKey] = prettyOfClass(unnamedParam(_.cause))
  }

  final case class FailedToSignViewMessage(cause: SyncCryptoError)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[FailedToSignViewMessage] = prettyOfClass(unnamedParam(_.cause))
  }

  final case class FailedToEncryptRandomness(cause: EncryptionError)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[FailedToEncryptRandomness] = prettyOfClass(unnamedParam(_.cause))
  }

  final case class FailedToEncryptViewMessage(cause: EncryptionError)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[FailedToEncryptViewMessage] = prettyOfClass(unnamedParam(_.cause))
  }

  final case class FailedToDeserializeEncryptedRandomness(cause: DeserializationError)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[FailedToDeserializeEncryptedRandomness] = prettyOfClass(
      unnamedParam(_.cause)
    )
  }

  /** Indicates that there is no encrypted session key randomness to be found
    */
  final case class UnableToDetermineSessionKeyRandomness(cause: String)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[UnableToDetermineSessionKeyRandomness] = prettyOfClass(
      param("cause", _.cause.unquoted)
    )
  }
}
