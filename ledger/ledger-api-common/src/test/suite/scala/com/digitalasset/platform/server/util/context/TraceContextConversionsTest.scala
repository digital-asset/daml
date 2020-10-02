// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.util.context

import com.daml.ledger.api.v1.trace_context.TraceContext
import org.scalatest.{Matchers, WordSpec}

class TraceContextConversionsTest extends WordSpec with Matchers {

  private val sut = TraceContextConversions

  "TraceContextConversionsTest" should {

    "convert symmetrically" in {
      val in = TraceContext(1L, 2L, 3L, Some(4L), sampled = true)

      val brave = sut.toBrave(in)
      val out = sut.toProto(brave)

      in shouldEqual out

    }

  }
}
