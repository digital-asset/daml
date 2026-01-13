// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import com.digitalasset.canton.crypto.{HashPurpose, SynchronizerCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.ExecutionContext

trait SignatureVerifier {
  def verifySignature[A <: ProtocolVersionedMemoizedEvidence](
      signedContent: SignedContent[A],
      hashPurpose: HashPurpose,
      sender: A => Member,
      estimatedSequencingTimestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    String,
    SignedContent[A],
  ]
}

object SignatureVerifier {
  def apply(
      cryptoApi: SynchronizerCryptoClient
  )(implicit executionContext: ExecutionContext): SignatureVerifier = new SignatureVerifier {
    override def verifySignature[A <: ProtocolVersionedMemoizedEvidence](
        signedContent: SignedContent[A],
        hashPurpose: HashPurpose,
        sender: A => Member,
        estimatedSequencingTimestamp: CantonTimestamp,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, SignedContent[A]] = {
      val headSnapshotTs = cryptoApi.headSnapshot.ipsSnapshot.timestamp
      val sequencingTimestampSuccessor = estimatedSequencingTimestamp.immediateSuccessor
      for {
        // If the current time is later than the head snapshot, create a hypothetical snapshot
        // (i.e., one referencing a timestamp possibly in the future) so that signature verification
        // uses the correct timestamp. This is necessary because submission requests are signed
        // using the local clock, so verification must be aligned accordingly.
        snapshotTuple <- EitherTUtil.fromFuture(
          if (sequencingTimestampSuccessor > headSnapshotTs)
            cryptoApi
              .hypotheticalSnapshot(
                headSnapshotTs,
                sequencingTimestampSuccessor,
              )
              .map((_, true))
          // TODO(#29607): Replace with the snapshot for the sequencing timestamp once `getTime` is fixed.
          else cryptoApi.currentSnapshotApproximation.map((_, false)),
          // cryptoApi.snapshot(sequencingTimestampSuccessor).map((_, false)),
          err => s"Could not obtain a snapshot to verify the signature: $err",
        )
        (snapshot, usingHypotheticalSnapshot) = snapshotTuple
        _ <- signedContent
          .verifySignature(
            snapshot,
            sender(signedContent.content),
            hashPurpose,
          )
          .leftMap { error =>
            val errMsg =
              s"Sequencer could not verify client's signature ${signedContent.timestampOfSigningKey
                  .fold("")(ts => s"at $ts ")}on request with sequencer's "
            if (usingHypotheticalSnapshot)
              errMsg + s"hypothetical snapshot (timestamp = $headSnapshotTs, " +
                s"desiredTimestamp =$sequencingTimestampSuccessor). Error: $error"
            else
              errMsg + s"current snapshot at $sequencingTimestampSuccessor. Error: $error"
          }
      } yield signedContent
    }
  }
}
