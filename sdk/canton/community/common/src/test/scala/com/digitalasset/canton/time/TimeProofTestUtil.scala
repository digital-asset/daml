// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.syntax.option.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, SignedContent}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, SequencerCounter}

object TimeProofTestUtil {
  def mkTimeProof(
      timestamp: CantonTimestamp,
      counter: Long = 0L,
      targetDomain: TargetDomainId = TargetDomainId(DefaultTestIdentities.domainId),
  ): TimeProof = {
    val deliver = Deliver.create(
      SequencerCounter(counter),
      timestamp,
      targetDomain.unwrap,
      TimeProof.mkTimeProofRequestMessageId.some,
      Batch.empty(BaseTest.testedProtocolVersion),
      BaseTest.testedProtocolVersion,
    )
    val signedContent =
      SignedContent(deliver, SymbolicCrypto.emptySignature, None, BaseTest.testedProtocolVersion)
    val event = OrdinarySequencedEvent(signedContent, None)(TraceContext.empty)
    TimeProof
      .fromEvent(event)
      .fold(err => sys.error(s"Failed to create time proof: $err"), identity)
  }
}
