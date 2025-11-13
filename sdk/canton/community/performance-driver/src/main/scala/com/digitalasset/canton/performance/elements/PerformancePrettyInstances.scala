// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements

import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.performance.model.java as M

import scala.jdk.CollectionConverters.CollectionHasAsScala

import Pretty.*

object PerformancePrettyInstances {
  implicit val prettyTestRun: Pretty[M.orchestration.TestRun] =
    prettyOfClass(
      param("mode", _.mode.toString.unquoted),
      param("issuers", _.issuers.asScala.toSeq.map(_.singleQuoted)),
      param("traders", _.traders.asScala.toSeq.map(_.singleQuoted)),
      param("master", _.master.singleQuoted),
      param("totalCycles", _.totalCycles),
      param("reportFrequency", _.reportFrequency),
    )

  implicit val prettyTestProbe: Pretty[M.orchestration.TestProbe] =
    prettyOfClass(
      param("master", _.master.singleQuoted),
      param("party", _.party.singleQuoted),
      param("typ", _.typ.toString.unquoted),
      param("timestamp", _.timestamp),
      param("count", _.count),
    )

  implicit val prettyTestParticipant: Pretty[M.orchestration.TestParticipant] =
    prettyOfClass(
      param("master", _.master.singleQuoted),
      param("party", _.party.singleQuoted),
      param("role", _.role.toString.unquoted),
      param("flag", _.flag.toString.unquoted),
      param("proposed", _.proposed),
      param("accepted", _.accepted),
    )

  implicit val prettyParticipationRequest: Pretty[M.orchestration.ParticipationRequest] =
    prettyOfClass(
      param("party", _.party.singleQuoted),
      param("role", _.role.toString.unquoted),
      param("master", _.master.singleQuoted),
    )

  implicit val prettyGenerator: Pretty[M.generator.Generator] =
    prettyOfClass(
      param("owner", _.owner.singleQuoted),
      param("processedIssuers", _.processedIssuers.asScala.toSeq.map(_.singleQuoted)),
    )

}
