// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.digitalasset.canton.BaseTestWordSpec
import org.scalatest.BeforeAndAfterEach

class SerializableTraceContextTest extends BaseTestWordSpec with BeforeAndAfterEach {

  var testTelemetrySetup: TestTelemetrySetup = _

  override def beforeEach(): Unit = {
    testTelemetrySetup = new TestTelemetrySetup()
  }

  override def afterEach(): Unit = {
    testTelemetrySetup.close()
  }

  "SerializableTraceContext" can {
    // If the trace has no fields set and `tc.toProtoV0.toByteArray` is used, the trace context will serialize to an
    // empty byte array. This is problematic as some databases will treat an empty byte array as null for their blob
    // columns but our table definitions typically expect non-null for the trace context column value.
    // This is not an issue when serializing a `VersionedTraceContext` but to not regress we have this unit test.
    "won't be serialized to an empty ByteArray" in {
      val res = SerializableTraceContext.empty.toByteArray(testedProtocolVersion)
      val empty = new Array[Byte](0)
      res should not be empty
    }

    "serialization roundtrip preserves equality" in {
      val rootSpan = testTelemetrySetup.tracer.spanBuilder("test").startSpan()
      val childSpan = testTelemetrySetup.tracer
        .spanBuilder("equality")
        .setParent(TraceContext.empty.context.`with`(rootSpan))
        .startSpan()

      val emptyContext = TraceContext.empty
      val contextWithRootSpan = TraceContext(emptyContext.context.`with`(rootSpan))
      val contextWithChildSpan = TraceContext(emptyContext.context.`with`(childSpan))

      val testCases = Seq(emptyContext, contextWithRootSpan, contextWithChildSpan)
      forEvery(testCases) { context =>
        SerializableTraceContext
          .fromProtoV0(SerializableTraceContext(context).toProtoV0) shouldBe
          Right(SerializableTraceContext(context))
        SerializableTraceContext.fromProtoVersioned(
          SerializableTraceContext(context).toProtoVersioned(testedProtocolVersion)
        ) shouldBe Right(SerializableTraceContext(context))
      }
    }
  }
}
