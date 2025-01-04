// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer

import cats.data.EitherT
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait SignatureVerifier {
  def verifySignature[A <: ProtocolVersionedMemoizedEvidence](
      signedContent: SignedContent[A],
      hashPurpose: HashPurpose,
      sender: A => Member,
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    String,
    SignedContent[A],
  ]
}

object SignatureVerifier {
  def apply(
      cryptoApi: DomainSyncCryptoClient
  )(implicit executionContext: ExecutionContext): SignatureVerifier = new SignatureVerifier {
    override def verifySignature[A <: ProtocolVersionedMemoizedEvidence](
        signedContent: SignedContent[A],
        hashPurpose: HashPurpose,
        sender: A => Member,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, SignedContent[A]] = {
      val snapshot = cryptoApi.headSnapshot(traceContext)
      val timestamp = snapshot.ipsSnapshot.timestamp
      signedContent
        .verifySignature(
          snapshot,
          sender(signedContent.content),
          hashPurpose,
        )
        .leftMap(error =>
          s"Sequencer could not verify client's signature ${signedContent.timestampOfSigningKey
              .fold("")(ts => s"at $ts ")}on request with sequencer's head snapshot at $timestamp. Error: $error"
        )
        .map(_ => signedContent)
    }
  }
}
