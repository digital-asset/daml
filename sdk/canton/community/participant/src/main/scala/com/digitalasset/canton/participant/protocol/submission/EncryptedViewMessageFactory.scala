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
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.store.SessionKeyStore.RecipientGroup
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.{HasToByteString, ProtocolVersion}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

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
          cryptoSnapshot
            .sign(rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
            .leftMap(err => FailedToSignViewMessage(err))
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
          .leftMap[EncryptedViewMessageCreationError](FailedToEncryptViewMessage.apply)
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

  final case class ViewParticipantsKeysAndParentRecipients(
      informeeParticipants: NonEmpty[Set[ParticipantId]],
      encryptionKeys: Set[Fingerprint],
      parentRecipients: Option[Recipients],
  )

  final case class RandomnessAndReference(
      randomness: SecureRandomness,
      reference: Object,
  )

  final case class RandomnessRevocationInfo(
      randomnessAndReference: RandomnessAndReference,
      encryptedBy: Option[Object],
      informeeParticipants: NonEmpty[Set[ParticipantId]],
      newKey: Boolean,
  )

  @VisibleForTesting
  private[canton] def computeSessionKeyRandomness(
      sessionKeyStoreSnapshot: Map[RecipientGroup, SessionKeyInfo],
      randomnessRevocationMap: Map[RecipientGroup, RandomnessRevocationInfo],
      recipients: Recipients,
      viewMetadata: ViewParticipantsKeysAndParentRecipients,
      pureCrypto: CryptoPureApi,
  ): Map[RecipientGroup, RandomnessRevocationInfo] = {

    val viewEncryptionScheme = pureCrypto.defaultSymmetricKeyScheme
    val randomnessLength = computeRandomnessLength(pureCrypto)

    // creates a brand-new session key randomness with the correct reference to the parent's randomness
    def generateNewSessionKeyRandomness(
        recipients: Recipients,
        encryptedBy: Option[Object],
        informeeParticipants: NonEmpty[Set[ParticipantId]],
    ): Map[RecipientGroup, RandomnessRevocationInfo] =
      randomnessRevocationMap.get(RecipientGroup(recipients, viewEncryptionScheme)) match {
        case Some(_) => randomnessRevocationMap
        case None =>
          val sessionKeyRandomness = pureCrypto.generateSecureRandomness(randomnessLength)
          val symbolicReference = new Object()

          // save the new randomness to our `state` map with the `new/revocation` flag set to true
          randomnessRevocationMap + (RecipientGroup(recipients, viewEncryptionScheme) ->
            RandomnessRevocationInfo(
              RandomnessAndReference(sessionKeyRandomness, symbolicReference),
              encryptedBy,
              informeeParticipants,
              newKey = true,
            ))
      }

    def getParentRandomnessReference(recipientsO: Option[Recipients]): Option[Object] =
      recipientsO.map { recipients =>
        randomnessRevocationMap(
          RecipientGroup(recipients, viewEncryptionScheme)
        ).randomnessAndReference.reference
      }

    // check that we already have a session key for a similar transaction that we can reuse
    sessionKeyStoreSnapshot.get(
      RecipientGroup(recipients, viewEncryptionScheme)
    ) match {
      case Some(sessionKeyInfo) =>
        // we need to check that the public keys match, or if they have been changed in the meantime. If they did
        // we are revoking the session key because that public key could have been compromised
        val allPubKeysIdsInMessage = sessionKeyInfo.encryptedSessionKeys.map(_.encryptedFor)
        // check that that all recipients are represented in the message, in other words,
        // that at least one of their active public keys is present in the sequence `encryptedSessionKeys`
        val checkActiveParticipantKeys =
          allPubKeysIdsInMessage.forall(viewMetadata.encryptionKeys.contains)

        // check that the reference to the parent's randomness is correct (parent randomness has not been revoked)
        // if we are dealing with non transaction requests (e.g. (un)assignments), there are is no parent randomness,
        // so we simply revoke the randomness if the keys are not active.
        val (parentRandomnessRef, checkEncryptedBy) = getParentRandomnessReference(
          viewMetadata.parentRecipients
        ) match {
          case Some(pRandomnessRef) =>
            (
              Some(pRandomnessRef),
              sessionKeyInfo.encryptedBy.contains(pRandomnessRef),
            )
          // it's a root node
          case None => (sessionKeyInfo.encryptedBy, true)
        }

        // if everything is correct we can use the randomness stored in the cache and store the entry in our `state`
        // map with the `new/revocation` flag set to false
        if (checkActiveParticipantKeys && checkEncryptedBy) {
          randomnessRevocationMap + (RecipientGroup(recipients, viewEncryptionScheme) ->
            RandomnessRevocationInfo(
              RandomnessAndReference(
                sessionKeyInfo.sessionKeyAndReference.randomness,
                sessionKeyInfo.sessionKeyAndReference.reference,
              ),
              sessionKeyInfo.encryptedBy,
              viewMetadata.informeeParticipants,
              newKey = false,
            ))
        } else
          // if not, we need to revoke the session key and save it with `new/revocation` flag set to true
          generateNewSessionKeyRandomness(
            recipients,
            parentRandomnessRef,
            viewMetadata.informeeParticipants,
          )
      case None =>
        // if not in cache we need to generate a new session key for this view and save it
        generateNewSessionKeyRandomness(
          recipients,
          getParentRandomnessReference(viewMetadata.parentRecipients),
          viewMetadata.informeeParticipants,
        )
    }
  }

  /** Generates session keys based on the recipients trees and on the values already cached.
    *
    * @param viewRecipients the list of views and their respective recipients, parent recipients and informees. For
    *                       this function to work correctly the views MUST be passed in PRE-ORDER.
    */
  def generateKeysFromRecipients(
      viewRecipients: Seq[(ViewHashAndRecipients, Option[Recipients], List[LfPartyId])],
      parallel: Boolean,
      pureCrypto: CryptoPureApi,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[
    FutureUnlessShutdown,
    EncryptedViewMessageCreationError,
    Map[ViewHash, ViewKeyData],
  ] = {
    val viewEncryptionScheme = pureCrypto.defaultSymmetricKeyScheme

    // create a snapshot of the data from the cache to use during key generation
    val allRecipientsGroup = viewRecipients.map { case (viewHashAndRecipients, _, _) =>
      RecipientGroup(viewHashAndRecipients.recipients, viewEncryptionScheme)
    }
    val sessionKeyStoreSnapshot = sessionKeyStore.getSessionKeysInfoIfPresent(allRecipientsGroup)

    def eitherTUS[B](
        value: Either[EncryptedViewMessageCreationError, B]
    ): EitherT[FutureUnlessShutdown, EncryptedViewMessageCreationError, B] =
      EitherT.fromEither[FutureUnlessShutdown](value)

    def encryptSessionKeyRandomness(
        sessionKeyRandomness: SecureRandomness,
        informeeParticipants: NonEmpty[Set[ParticipantId]],
    ): EitherT[
      FutureUnlessShutdown,
      EncryptedViewMessageCreationError,
      ViewKeyData,
    ] =
      for {
        sessionKey <- eitherTUS(
          pureCrypto
            .createSymmetricKey(sessionKeyRandomness, viewEncryptionScheme)
            .leftMap(FailedToCreateEncryptionKey.apply)
        )
        // generates the session key map, which contains the session key randomness encrypted for all informee participants
        sessionKeyMap <- createDataMap(
          informeeParticipants.forgetNE.to(LazyList),
          sessionKeyRandomness,
          cryptoSnapshot,
        ).map(_.values.toSeq)
      } yield ViewKeyData(sessionKeyRandomness, sessionKey, sessionKeyMap)

    def getInformeeParticipantsAndKeys(
        informeeParties: List[LfPartyId]
    ): EitherT[
      FutureUnlessShutdown,
      EncryptedViewMessageCreationError,
      (Set[ParticipantId], Set[Fingerprint]),
    ] =
      for {
        informeeParticipants <- cryptoSnapshot.ipsSnapshot
          .activeParticipantsOfAll(informeeParties)
          .leftMap(UnableToDetermineParticipant(_, cryptoSnapshot.domainId))
        memberEncryptionKeys <- EitherT
          .right[EncryptedViewMessageCreationError](
            cryptoSnapshot.ipsSnapshot
              .encryptionKeys(informeeParticipants.toSeq)
          )

        memberEncryptionKeysIds = memberEncryptionKeys.flatMap { case (_, keys) =>
          keys.map(_.id)
        }.toSet

      } yield (informeeParticipants, memberEncryptionKeysIds)

    def mkSessionKeyData(
        recipientGroup: RecipientGroup,
        randomnessRevocationInfo: RandomnessRevocationInfo,
    ): EitherT[
      FutureUnlessShutdown,
      EncryptedViewMessageCreationError,
      (RecipientGroup, (ViewKeyData, Object, Option[Object], Boolean)),
    ] =
      for {
        sessionKeyData <-
          // if it's a new randomness with reference to the cache we encrypt it
          if (randomnessRevocationInfo.newKey) {
            encryptSessionKeyRandomness(
              randomnessRevocationInfo.randomnessAndReference.randomness,
              randomnessRevocationInfo.informeeParticipants,
            ).map(
              (
                _,
                randomnessRevocationInfo.randomnessAndReference.reference,
                randomnessRevocationInfo.encryptedBy,
                true,
              )
            )
          } // if it's not a new randomness we re-use its encryption from the cache
          else
            EitherT.rightT[FutureUnlessShutdown, EncryptedViewMessageCreationError] {
              val sessionKeyInfo = sessionKeyStoreSnapshot(recipientGroup)
              (
                ViewKeyData(
                  sessionKeyInfo.sessionKeyAndReference.randomness,
                  sessionKeyInfo.sessionKeyAndReference.key,
                  sessionKeyInfo.encryptedSessionKeys,
                ),
                sessionKeyInfo.sessionKeyAndReference.reference,
                sessionKeyInfo.encryptedBy,
                false,
              )
            }
      } yield recipientGroup -> sessionKeyData

    // we start from top to bottom of the tree (i.e., pre-order) so the parent's randomness is created before it's
    // actually needed
    for {
      viewRecipientsAndInformeeParticipants <- viewRecipients.parTraverse {
        case (vhR, parentRecipientsO, informees) =>
          getInformeeParticipantsAndKeys(informees).flatMap {
            case (informeeParticipants, encryptionKeys) =>
              NonEmpty
                .from(informeeParticipants)
                .toRight(
                  UnableToDetermineRecipients(
                    "The list of informee participants is empty"
                  ): EncryptedViewMessageCreationError
                )
                .map(informeeParticipantsNE =>
                  vhR -> ViewParticipantsKeysAndParentRecipients(
                    informeeParticipantsNE,
                    encryptionKeys,
                    parentRecipientsO,
                  )
                )
                .toEitherT[FutureUnlessShutdown]
          }
      }

      // this map keeps track of the randomnesses that we generate/revoke or use directly from the cache
      // the generation of the randomness for the encryption session keys has to be done sequentially,
      // because we need to revoke the topmost views' keys before so their children can also revoke their keys.
      randomnessRevocationMap = viewRecipientsAndInformeeParticipants
        .foldLeft(Map.empty[RecipientGroup, RandomnessRevocationInfo]) {
          case (stateMap, (viewHashAndRecipients, viewMetadata)) =>
            computeSessionKeyRandomness(
              sessionKeyStoreSnapshot,
              stateMap,
              viewHashAndRecipients.recipients,
              viewMetadata,
              pureCrypto,
            )
        }
      viewKeyDataWithReferences <-
        if (parallel)
          randomnessRevocationMap.toList
            .parTraverse { case (recipientGroup, randomnessRevocationInfo) =>
              mkSessionKeyData(
                recipientGroup,
                randomnessRevocationInfo,
              )
            }
            .map(_.toMap)
        else
          MonadUtil
            .sequentialTraverse(randomnessRevocationMap.toList) {
              case (recipientGroup, randomnessRevocationInfo) =>
                mkSessionKeyData(
                  recipientGroup,
                  randomnessRevocationInfo,
                )
            }
            .map(_.toMap)

      viewKeyData = viewRecipients.map { case (vhr, _, _) =>
        val (viewKeyData, _, _, _) =
          viewKeyDataWithReferences(RecipientGroup(vhr.recipients, viewEncryptionScheme))
        vhr.viewHash -> viewKeyData
      }.toMap

      // save all data to cache to be used by other transactions
      sessionKeysInfoMap = viewKeyDataWithReferences.collect {
        case (recipientsGroup, (vkd, ref, parentRef, newKey)) if newKey =>
          recipientsGroup ->
            SessionKeyInfo(
              SessionKeyAndReference(vkd.viewKeyRandomness, vkd.viewKey, ref),
              parentRef,
              vkd.viewKeyRandomnessMap,
            )
      }
      _ = sessionKeyStore.saveSessionKeysInfo(sessionKeysInfoMap)
    } yield viewKeyData

  }

  private def createDataMap[M <: HasToByteString](
      participants: LazyList[ParticipantId],
      data: M,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageCreationError, Map[
    ParticipantId,
    AsymmetricEncrypted[M],
  ]] =
    cryptoSnapshot
      .encryptFor(data, participants)
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
    override protected def pretty: Pretty[UnableToDetermineRecipients] = prettyOfClass(
      param("cause", _.cause.unquoted)
    )
  }

  /** Indicates that the participant hosting one or more informees could not be determined.
    */
  final case class UnableToDetermineParticipant(party: Set[LfPartyId], domain: DomainId)
      extends EncryptedViewMessageCreationError {
    override protected def pretty: Pretty[UnableToDetermineParticipant] =
      prettyOfClass(unnamedParam(_.party), unnamedParam(_.domain))
  }

  /** Indicates that the public key of an informee participant could not be determined.
    */
  final case class UnableToDetermineKey(
      participant: ParticipantId,
      cause: SyncCryptoError,
      domain: DomainId,
  ) extends EncryptedViewMessageCreationError {
    override protected def pretty: Pretty[UnableToDetermineKey] = prettyOfClass(
      param("participant", _.participant),
      param("cause", _.cause),
    )
  }

  final case class FailedToCreateEncryptionKey(cause: EncryptionKeyCreationError)
      extends EncryptedViewMessageCreationError {
    override protected def pretty: Pretty[FailedToCreateEncryptionKey] = prettyOfClass(
      unnamedParam(_.cause)
    )
  }

  final case class FailedToSignViewMessage(cause: SyncCryptoError)
      extends EncryptedViewMessageCreationError {
    override protected def pretty: Pretty[FailedToSignViewMessage] = prettyOfClass(
      unnamedParam(_.cause)
    )
  }

  final case class FailedToEncryptViewMessage(cause: EncryptionError)
      extends EncryptedViewMessageCreationError {
    override protected def pretty: Pretty[FailedToEncryptViewMessage] = prettyOfClass(
      unnamedParam(_.cause)
    )
  }

  /** Indicates that there is no encrypted session key randomness to be found
    */
  final case class UnableToDetermineSessionKeyRandomness(cause: String)
      extends EncryptedViewMessageCreationError {
    override protected def pretty: Pretty[UnableToDetermineSessionKeyRandomness] = prettyOfClass(
      param("cause", _.cause.unquoted)
    )
  }
}
