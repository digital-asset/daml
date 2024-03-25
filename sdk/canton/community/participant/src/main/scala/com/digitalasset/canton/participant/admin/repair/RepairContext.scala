// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String255,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.tracing.TraceContext

/** Make tracecontext mandatory throughout repair operations.
  *
  * @param str the w3c serialized tracing information of the trace parent
  *            The W3C standard specifies that the traceparent is length-limited -> thus it is safe to limit it to 255 characters
  *            However, Tracestates aren't limited, so the repair context should be saved as a blob (like [[com.digitalasset.canton.tracing.TraceContext]])
  *            if we want to use it for repair contexts
  */
final case class RepairContext(override protected val str: String255)
    extends LengthLimitedStringWrapper
    with PrettyPrinting {
  def toLengthLimitedString: String255 = str

  override def pretty: Pretty[RepairContext] = prettyOfClass(unnamedParam(_.str.unwrap.unquoted))
}

object RepairContext extends LengthLimitedStringWrapperCompanion[String255, RepairContext] {

  def tryFromTraceContext(implicit traceContext: TraceContext): RepairContext =
    RepairContext(
      // take the serialized parent value which should contain
      traceContext.asW3CTraceContext
        .map(tc => String255.tryCreate(tc.parent, Some("RepairContext")))
        .getOrElse(
          throw new IllegalArgumentException(
            "RepairService cannot be invoked with an empty trace context"
          )
        )
    )

  override def instanceName: String = "RepairContext"

  override protected def companion: String255.type = String255

  override protected def factoryMethodWrapper(str: String255): RepairContext = RepairContext(str)
}
