// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{
  EncryptedView,
  EncryptedViewMessage,
  EncryptedViewMessageV0,
  EncryptedViewMessageV1,
  RootHashMessageRecipients,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

object EncryptedViewMessageFactory {

  def create[VT <: ViewType](viewType: VT)(
      viewTree: viewType.View,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      protocolVersion: ProtocolVersion,
      optRandomness: Option[SecureRandomness] = None,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, EncryptedViewMessageCreationError, EncryptedViewMessage[VT]] = {

    val cryptoPureApi = cryptoSnapshot.pureCrypto

    val viewEncryptionScheme = cryptoPureApi.defaultSymmetricKeyScheme
    val viewKeyLength = viewEncryptionScheme.keySizeInBytes
    val randomnessLength = EncryptedViewMessage.computeRandomnessLength(cryptoPureApi)
    val randomness: SecureRandomness =
      optRandomness.getOrElse(cryptoPureApi.generateSecureRandomness(randomnessLength))

    val informeeParties = viewTree.informees.map(_.party).toList

    def eitherT[B](
        value: Either[EncryptedViewMessageCreationError, B]
    ): EitherT[Future, EncryptedViewMessageCreationError, B] =
      EitherT.fromEither[Future](value)

    for {
      symmetricViewKeyRandomness <- eitherT(
        cryptoPureApi
          .computeHkdf(randomness.unwrap, viewKeyLength, HkdfInfo.ViewKey)
          .leftMap(FailedToExpandKey)
      )
      symmetricViewKey <- eitherT(
        cryptoPureApi
          .createSymmetricKey(symmetricViewKeyRandomness, viewEncryptionScheme)
          .leftMap(FailedToCreateEncryptionKey)
      )
      recipientsInfo <- RootHashMessageRecipients
        .encryptedViewMessageRecipientsInfo(
          cryptoSnapshot.ipsSnapshot,
          informeeParties,
        )
        .leftMap(UnableToDetermineParticipant(_, cryptoSnapshot.domainId))
      usingGroupAddressing = recipientsInfo.partiesWithGroupAddressing.nonEmpty
      randomnessMap <-
        if (!usingGroupAddressing)
          createRandomnessMap(
            recipientsInfo.informeeParticipants.to(LazyList),
            randomness,
            cryptoSnapshot,
            protocolVersion,
          )
        else
          EitherT.rightT[Future, EncryptedViewMessageCreationError](
            Map.empty[
              ParticipantId,
              AsymmetricEncrypted[SecureRandomness],
            ]
          )
      signature <- viewTree.toBeSigned
        .traverse(rootHash => cryptoSnapshot.sign(rootHash.unwrap).leftMap(FailedToSignViewMessage))
      encryptedView <- eitherT(
        EncryptedView
          .compressed[VT](cryptoPureApi, symmetricViewKey, viewType, protocolVersion)(viewTree)
          .leftMap(FailedToEncryptViewMessage)
      )
      message = {
        if (protocolVersion >= ProtocolVersion.v4) {
          val randomnessV1 =
            if (!usingGroupAddressing) randomnessMap.values.toSeq
            else
              Seq(
                AsymmetricEncrypted[SecureRandomness](
                  randomness.unwrap,
                  AsymmetricEncrypted.noEncryptionFingerprint,
                )
              )
          EncryptedViewMessageV1[VT](
            signature,
            viewTree.viewHash,
            randomnessV1,
            encryptedView,
            viewTree.domainId,
            viewEncryptionScheme,
          )(
            Some(recipientsInfo)
          )
        } else {
          val randomnessMapV0 = randomnessMap.fmap(_.encrypted)
          EncryptedViewMessageV0[VT](
            signature,
            viewTree.viewHash,
            randomnessMapV0,
            encryptedView,
            viewTree.domainId,
          )
        }
      }
    } yield message
  }

  private def createRandomnessMap(
      participants: LazyList[ParticipantId],
      randomness: SecureRandomness,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      version: ProtocolVersion,
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, EncryptedViewMessageCreationError, Map[
    ParticipantId,
    AsymmetricEncrypted[SecureRandomness],
  ]] =
    participants
      .parTraverse { participant =>
        cryptoSnapshot
          .encryptFor(randomness, participant, version)
          .bimap(
            UnableToDetermineKey(
              participant,
              _,
              cryptoSnapshot.domainId,
            ): EncryptedViewMessageCreationError,
            participant -> _,
          )
      }
      .map(_.toMap)

  sealed trait EncryptedViewMessageCreationError
      extends Product
      with Serializable
      with PrettyPrinting

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

  final case class FailedToEncryptViewMessage(cause: EncryptionError)
      extends EncryptedViewMessageCreationError {
    override def pretty: Pretty[FailedToEncryptViewMessage] = prettyOfClass(unnamedParam(_.cause))
  }
}
