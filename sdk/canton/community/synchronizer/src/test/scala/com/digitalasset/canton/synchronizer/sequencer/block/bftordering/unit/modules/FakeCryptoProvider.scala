// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{
  Hash,
  HashPurpose,
  Signature,
  SignatureCheckError,
  SigningKeyUsage,
  SyncCryptoError,
}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.Assertions.fail

class FakeCryptoProvider[E <: Env[E]] extends CryptoProvider[E] {
  override def sign(hash: Hash, usage: NonEmpty[Set[SigningKeyUsage]])(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, Signature]] =
    fail("Module should not sign messages")

  override def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence with MessageFrom](
      message: MessageT,
      hashPurpose: HashPurpose,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, SignedMessage[MessageT]]] =
    fail("Module should not sign messages")

  override def verifySignature(hash: Hash, member: SequencerId, signature: Signature)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SignatureCheckError, Unit]] =
    fail("Module should not verifySignature messages")
}
