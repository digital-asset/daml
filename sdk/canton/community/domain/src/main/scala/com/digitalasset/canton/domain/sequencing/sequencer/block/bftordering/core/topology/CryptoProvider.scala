// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology

import com.digitalasset.canton.crypto.{Hash, Signature, SignatureCheckError, SyncCryptoError}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

trait CryptoProvider[E <: Env[E]] {
  def sign(hash: Hash)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, Signature]]

  def verifySignature(hash: Hash, member: SequencerId, signature: Signature)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SignatureCheckError, Unit]]
}
