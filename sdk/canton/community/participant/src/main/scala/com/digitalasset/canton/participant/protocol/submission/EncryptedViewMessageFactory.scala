// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{SecureRandomness, SymmetricKey, *}
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ViewHash
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.computeRandomnessLength
import com.digitalasset.canton.protocol.messages.{EncryptedView, EncryptedViewMessage}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.store.SessionKeyStore.RecipientGroup
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}

import scala.concurrent.{ExecutionContext, Future}

object EncryptedViewMessageFactory {

  final case class ViewHashAndRecipients(
      viewHash: ViewHash,
      recipients: Recipients,
  )

  final case class ViewKeyData(
      viewKeyRandomness: SecureRandomness,
      viewKey: SymmetricKey,
      viewKeyRandomnessMap: Seq[AsymmetricEncrypted[SecureRandomness]],
  )

  def create[VT <: ViewType](viewType: VT)(
      viewTree: viewType.View,
      viewKeyData: (SymmetricKey, Seq[AsymmetricEncrypted[SecureRandomness]]),
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageCreationError, EncryptedViewMessage[VT]] =
    for {
      signature <- viewTree.toBeSigned
        .parTraverse(rootHash =>
          cryptoSnapshot.sign(rootHash.unwrap).leftMap(FailedToSignViewMessage)
        )
      (sessionKey, sessionKeyRandomnessMap) = viewKeyData
      sessionKeyRandomnessMapNE <- EitherT.fromEither[FutureUnlessShutdown](
        NonEmpty
          .from(sessionKeyRandomnessMap)
          .toRight(
            UnableToDetermineSessionKeyRandomness(
              "The session key randomness map is empty"
            )
          )
      )
      encryptedView <- EitherT.fromEither[FutureUnlessShutdown](
        EncryptedView
          .compressed[VT](cryptoSnapshot.pureCrypto, sessionKey, viewType)(viewTree)
          .leftMap[EncryptedViewMessageCreationError](FailedToEncryptViewMessage)
      )
    } yield EncryptedViewMessage[VT](
      signature,
      viewTree.viewHash,
      sessionKeyRandomnessMapNE,
      encryptedView,
      viewTree.domainId,
      cryptoSnapshot.pureCrypto.defaultSymmetricKeyScheme,
      protocolVersion,
    )

  // TODO(#21392): Use the same session key within a transaction even if cache is disabled.
  def generateKeysFromRecipients(
      viewRecipients: Seq[(ViewHashAndRecipients, List[LfPartyId])],
      parallel: Boolean,
      pureCrypto: CryptoPureApi,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
      protocolVersion: ProtocolVersion,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[
    FutureUnlessShutdown,
    EncryptedViewMessageCreationError,
    Map[ViewHash, ViewKeyData],
  ] = {
    val viewEncryptionScheme = pureCrypto.defaultSymmetricKeyScheme
    val randomnessLength = computeRandomnessLength(pureCrypto)

    def eitherTUS[B](
        value: Either[EncryptedViewMessageCreationError, B]
    ): EitherT[FutureUnlessShutdown, EncryptedViewMessageCreationError, B] =
      EitherT.fromEither[FutureUnlessShutdown](value)

    def getRecipientInfo(
        informeeParties: List[LfPartyId]
    ): EitherT[FutureUnlessShutdown, EncryptedViewMessageFactory.UnableToDetermineParticipant, Set[
      ParticipantId
    ]] =
      for {
        informeeParticipants <- cryptoSnapshot.ipsSnapshot
          .activeParticipantsOfAll(informeeParties)
          .leftMap(UnableToDetermineParticipant(_, cryptoSnapshot.domainId))
          .mapK(FutureUnlessShutdown.outcomeK)
      } yield informeeParticipants

    def generateAndEncryptSessionKeyRandomness(
        recipients: Recipients,
        informeeParticipants: NonEmpty[Set[ParticipantId]],
    ): EitherT[
      FutureUnlessShutdown,
      EncryptedViewMessageCreationError,
      ViewKeyData,
    ] = {
      val sessionKeyRandomness = pureCrypto.generateSecureRandomness(randomnessLength)
      for {
        sessionKey <- eitherTUS(
          pureCrypto
            .createSymmetricKey(sessionKeyRandomness, viewEncryptionScheme)
            .leftMap(FailedToCreateEncryptionKey)
        )
        // generates the session key map, which contains the session key randomness encrypted for all recipients
        sessionKeyMap <- createDataMap(
          informeeParticipants.forgetNE.to(LazyList),
          sessionKeyRandomness,
          cryptoSnapshot,
          protocolVersion,
        ).map(_.values.toSeq).mapK(FutureUnlessShutdown.outcomeK)
        _ = sessionKeyStore.saveSessionKeyInfo(
          RecipientGroup(recipients, viewEncryptionScheme),
          SessionKeyInfo(sessionKeyRandomness, sessionKeyMap),
        )
      } yield ViewKeyData(sessionKeyRandomness, sessionKey, sessionKeyMap)
    }

    def getSessionKey(
        recipients: Recipients,
        informeeParticipants: Set[ParticipantId],
    ): EitherT[
      FutureUnlessShutdown,
      EncryptedViewMessageCreationError,
      ViewKeyData,
    ] =
      for {
        informeeParticipantsNE <- eitherTUS(
          NonEmpty
            .from(informeeParticipants)
            .toRight(UnableToDetermineRecipients("The list of informee participants is empty"))
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
                        .encryptionKeys(informeeParticipantsNE.forgetNE.toSeq)
                        .map { memberToKeysMap =>
                          informeeParticipantsNE.map(participant =>
                            memberToKeysMap
                              .getOrElse(participant, Seq.empty)
                              .exists(key => allPubKeysIdsInMessage.contains(key.id))
                          )
                        }
                    )
                    .map(_.forall(_ == true))
                    .mapK(FutureUnlessShutdown.outcomeK)
                // all public keys used to encrypt the session key must be present and active for each recipient
                sessionKeyData <-
                  if (checkActiveParticipantKeys)
                    eitherTUS(
                      pureCrypto
                        .createSymmetricKey(
                          sessionKeyInfo.sessionKeyRandomness,
                          viewEncryptionScheme,
                        )
                        .leftMap(FailedToCreateEncryptionKey)
                        .map(sessionKey =>
                          ViewKeyData(
                            sessionKeyInfo.sessionKeyRandomness,
                            sessionKey,
                            sessionKeyInfo.encryptedSessionKeys,
                          )
                        )
                    )
                  else
                    generateAndEncryptSessionKeyRandomness(recipients, informeeParticipantsNE)
              } yield sessionKeyData
            case None =>
              // if not in cache we need to generate a new session key for this view and save it
              generateAndEncryptSessionKeyRandomness(recipients, informeeParticipantsNE)
          }
      } yield sessionKeyAndMap

    def mkSessionKey(
        viewAndInformees: (ViewHashAndRecipients, List[LfPartyId])
    ): EitherT[FutureUnlessShutdown, EncryptedViewMessageCreationError, (ViewHash, ViewKeyData)] = {
      val (ViewHashAndRecipients(viewHash, recipients), informees) = viewAndInformees
      for {
        informeeParticipants <- getRecipientInfo(informees)
        sessionKeyData <- getSessionKey(recipients, informeeParticipants)
      } yield viewHash -> sessionKeyData
    }

    if (parallel) viewRecipients.parTraverse(mkSessionKey).map(_.toMap)
    else MonadUtil.sequentialTraverse(viewRecipients.toList)(mkSessionKey).map(_.toMap)
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

  final case class FailedToCreateEncryptionKey(cause: EncryptionKeyCreationError)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[FailedToCreateEncryptionKey] = prettyOfClass(
      unnamedParam(_.cause)
    )
  }

  final case class FailedToSignViewMessage(cause: SyncCryptoError)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[FailedToSignViewMessage] = prettyOfClass(unnamedParam(_.cause))
  }

  final case class FailedToEncryptViewMessage(cause: EncryptionError)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[FailedToEncryptViewMessage] = prettyOfClass(unnamedParam(_.cause))
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
