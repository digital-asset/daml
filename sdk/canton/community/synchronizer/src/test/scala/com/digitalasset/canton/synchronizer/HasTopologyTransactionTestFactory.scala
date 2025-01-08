// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer

import com.digitalasset.canton.crypto.{Fingerprint, HashPurpose, Signature, SigningKeyUsage}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.sequencing.sequencer.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.Sequencer.SignedOrderingRequest
import com.digitalasset.canton.topology.processing.TopologyTransactionTestFactory
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, HasExecutorService}
import com.google.protobuf.ByteString

import scala.concurrent.Future

trait HasTopologyTransactionTestFactory {
  self: BaseTest & HasExecutionContext & HasExecutorService =>

  protected final lazy val topologyTransactionFactory =
    new TopologyTransactionTestFactory(loggerFactory, executorService)
  // using lazy val's to avoid eager initialization of topologyTransactionFactory, in case the protocol version is < dev
  protected final lazy val SigningKeys = topologyTransactionFactory.SigningKeys
  protected final val participant1Key = topologyTransactionFactory.SigningKeys.key5

  protected final val ts0 = CantonTimestamp.Epoch
  protected final val ts1 = ts0.plusSeconds(10)

  protected final def sequencerSignedAndSenderSignedSubmissionRequest(
      sender: Member
  ): Future[SignedOrderingRequest] =
    sequencerSignedAndSenderSignedSubmissionRequest(sender, Recipients.cc(sender))

  protected final def sequencerSignedAndSenderSignedSubmissionRequest(
      sender: Member,
      recipients: Recipients,
      messageId: MessageId = MessageId.randomMessageId(),
      topologyTimestamp: Option[CantonTimestamp] = None,
      badEnvelopeSignature: Boolean = false,
      signingKey: Fingerprint = participant1Key.fingerprint,
      maxSequencingTime: CantonTimestamp = CantonTimestamp.MaxValue,
      aggregationRule: Option[AggregationRule] = None,
  ): Future[SignedOrderingRequest] =
    for {
      request <- submissionRequest(
        sender,
        recipients,
        messageId,
        topologyTimestamp,
        badEnvelopeSignature,
        signingKey,
        maxSequencingTime,
        aggregationRule,
      )
      hash =
        topologyTransactionFactory.cryptoApi.crypto.pureCrypto.digest(
          HashPurpose.SubmissionRequestSignature,
          request.getCryptographicEvidence,
        )
      signed <- topologyTransactionFactory.cryptoApi.crypto.privateCrypto
        .sign(hash, signingKey, SigningKeyUsage.ProtocolOnly)
        .map(signature =>
          SignedContent(
            request,
            signature,
            Some(ts1),
            testedProtocolVersion,
          )
        )
        .leftMap(_.toString)
        .value
        .failOnShutdown
        .map(_.value)
    } yield SignedContent(
      OrderingRequest.create(DefaultTestIdentities.sequencerId, signed, testedProtocolVersion),
      Signature.noSignature,
      Some(ts0.immediateSuccessor),
      testedProtocolVersion,
    )

  protected final def submissionRequest(
      sender: Member,
      recipients: Recipients,
      messageId: MessageId = MessageId.randomMessageId(),
      topologyTimestamp: Option[CantonTimestamp] = None,
      badEnvelopeSignature: Boolean = false,
      envelopeSigningKey: Fingerprint = topologyTransactionFactory.SigningKeys.key5.fingerprint,
      maxSequencingTime: CantonTimestamp = CantonTimestamp.MaxValue,
      aggregationRule: Option[AggregationRule] = None,
  ): Future[SubmissionRequest] =
    for {
      envelope <- signEnvelope(
        ClosedEnvelope.create(ByteString.EMPTY, recipients, Seq.empty, testedProtocolVersion),
        badEnvelopeSignature,
        envelopeSigningKey,
      )
    } yield SubmissionRequest.tryCreate(
      sender,
      messageId,
      Batch[ClosedEnvelope](
        List(
          envelope
        ),
        testedProtocolVersion,
      ),
      maxSequencingTime,
      topologyTimestamp,
      aggregationRule,
      Option.empty[SequencingSubmissionCost],
      testedProtocolVersion,
    )

  protected final def signEnvelope(
      envelope: ClosedEnvelope,
      badEnvelopeSignature: Boolean = false,
      envelopeSigningKey: Fingerprint = topologyTransactionFactory.SigningKeys.key5.fingerprint,
  ): Future[ClosedEnvelope] = {
    val bytes = if (badEnvelopeSignature) {
      ByteString.copyFromUtf8("wrong content")
    } else {
      envelope.bytes
    }
    val hash = topologyTransactionFactory.cryptoApi.crypto.pureCrypto
      .digest(HashPurpose.SignedProtocolMessageSignature, bytes)
    topologyTransactionFactory.cryptoApi.crypto.privateCrypto
      .sign(hash, envelopeSigningKey, SigningKeyUsage.ProtocolOnly)
      .valueOrFailShutdown(s"Failed to sign $envelope")
      .map(sig => envelope.copy(signatures = Seq(sig)))
  }
}
