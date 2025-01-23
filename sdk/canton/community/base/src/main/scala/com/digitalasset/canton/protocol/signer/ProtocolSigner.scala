// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.signer

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{
  Hash,
  Signature,
  SignatureCheckError,
  SigningKeyUsage,
  SyncCryptoError,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

/** Aggregates all methods related to protocol signing and signature verification. These methods require a topology snapshot
  * to ensure the correct signing keys are used, based on the current state (i.e., OwnerToKeyMappings).
  */
trait ProtocolSigner {

  protected val verificationParallelismLimit = 10

  def sign(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature]

  def verifySignature(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  def verifySignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  def verifyGroupSignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signers: Seq[Member],
      threshold: PositiveInt,
      groupName: String,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

}
