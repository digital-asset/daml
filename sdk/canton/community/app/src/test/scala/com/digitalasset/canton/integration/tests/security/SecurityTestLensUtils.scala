// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.{CryptoPureApi, SyncCryptoApi}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.MerkleTree.VersionedMerkleTree
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.{
  ClosedEnvelope,
  OpenEnvelope,
  Recipients,
  SubmissionRequest,
}
import monocle.macros.GenLens
import monocle.{Lens, Traversal}
import org.scalactic.source.Position

import scala.concurrent.ExecutionContext

/** A collection of Monocle utility methods used in security integration tests. */
trait SecurityTestLensUtils {
  this: BaseTest =>

  def pureCrypto: CryptoPureApi

  /** Traversal for the messages embedded in [[SignedProtocolMessage]]. Fails if the submission
    * request contains messages that are not of type [[SignedProtocolMessage]]. If the content of a
    * [[SignedProtocolMessage]] is not of type `M`, the traversal may still succeed due to erasure;
    * however, downstream code will likely fail with a [[java.lang.ClassCastException]] in that
    * case.
    */
  def traverseMessages[M <: SignedProtocolMessageContent](
      updateSignatureWith: M => Option[SyncCryptoApi]
  )(implicit
      executionContext: ExecutionContext
  ): Traversal[SubmissionRequest, M] =
    traverseSignedProtocolMessages[M]
      .andThen(
        Lens[SignedProtocolMessage[M], M](
          _.typedMessage.content
        ) { newMessage => signedMessage =>
          val newTypedMessage = signedMessage.typedMessage.copy(content = newMessage)
          updateSignatureWith(newMessage) match {
            case Some(snapshot) =>
              val newSig = SignedProtocolMessage
                .mkSignature(newTypedMessage, snapshot)
                .failOnShutdown
                .futureValue
              signedMessage.copy(typedMessage = newTypedMessage, signatures = NonEmpty(Seq, newSig))
            case None => signedMessage.copy(typedMessage = newTypedMessage)
          }
        }
      )

  /** Traversal for signed protocol messages. Fails if the submission request contains messages that
    * are not of type [[SignedProtocolMessage]]. If the content of a [[SignedProtocolMessage]] is
    * not of type `M`, the traversal may still succeed due to erasure; however, downstream code will
    * likely fail with a [[java.lang.ClassCastException]] in that case.
    */
  def traverseSignedProtocolMessages[M <: SignedProtocolMessageContent]
      : Traversal[SubmissionRequest, SignedProtocolMessage[M]] =
    GenLens[SubmissionRequest](_.batch.envelopes)
      .andThen(Traversal.fromTraverse[List, ClosedEnvelope])
      .andThen(ClosedEnvelope.tryDefaultOpenEnvelope(pureCrypto, testedProtocolVersion))
      .andThen(
        Lens[DefaultOpenEnvelope, SignedProtocolMessage[M]](
          _.protocolMessage.asInstanceOf[SignedProtocolMessage[M]]
        )(newMessage => _.copy(protocolMessage = newMessage))
      )

  /** Lens for focusing on the first element of a MerkleSeq. Fails if the seq is empty or the first
    * element is blinded.
    */
  def firstElement[M <: VersionedMerkleTree[M]](implicit pos: Position): Lens[MerkleSeq[M], M] =
    Lens[MerkleSeq[M], M](
      _.toSeq.headOption
        .valueOrFail("Unable to get the first element of the empty MerkleSeq.")
        .unwrap
        .valueOrFail("Unable to get the first element, because it is blinded.")
    )(newFirst =>
      merkleSeq =>
        (merkleSeq.toSeq: @unchecked) match {
          case Seq() =>
            fail("Unable to update the first element of the empty MerkleSeq.")
          case (_: BlindedNode[?]) +: _ =>
            fail("Unable to update the first element, because it is blinded.")
          case _ +: tail =>
            MerkleSeq.fromSeq(pureCrypto, testedProtocolVersion)(newFirst +: tail)
        }
    )

  def firstViewCommonData: Lens[GenTransactionTree, ViewCommonData] =
    GenTransactionTree.rootViewsUnsafe
      .andThen(firstElement[TransactionView])
      .andThen(TransactionView.viewCommonDataUnsafe)
      .andThen(MerkleTree.tryUnwrap[ViewCommonData])

  def allViewRecipients: Traversal[TransactionConfirmationRequest, Recipients] =
    GenLens[TransactionConfirmationRequest](_.viewEnvelopes)
      .andThen(Traversal.fromTraverse[Seq, OpenEnvelope[TransactionViewMessage]])
      .andThen(
        GenLens[OpenEnvelope[TransactionViewMessage]](_.recipients): Lens[
          OpenEnvelope[TransactionViewMessage],
          Recipients,
        ]
      )
}
