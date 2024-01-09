// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.syntax.option.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, SignedContent, TimeProof}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, SequencerCounter}

object TimeProofTestUtil {
  def mkTimeProof(
      timestamp: CantonTimestamp,
      counter: Long = 0L,
      targetDomain: TargetDomainId = TargetDomainId(DefaultTestIdentities.domainId),
      protocolVersion: ProtocolVersion = BaseTest.testedProtocolVersion,
  ): TimeProof = {
    val deliver = Deliver.create(
      SequencerCounter(counter),
      timestamp,
      targetDomain.unwrap,
      TimeProof.mkTimeProofRequestMessageId.some,
      Batch.empty(protocolVersion),
      protocolVersion,
    )
    val signedContent =
      SignedContent(deliver, SymbolicCrypto.emptySignature, None, protocolVersion)
    val event = OrdinarySequencedEvent(signedContent, None)(TraceContext.empty)
    TimeProof
      .fromEvent(event)
      .fold(err => sys.error(s"Failed to create time proof: $err"), identity)
  }
}
