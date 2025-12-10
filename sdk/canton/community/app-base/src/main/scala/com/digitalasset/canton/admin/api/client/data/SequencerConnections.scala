// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.SubmissionRequestAmplification as SubmissionRequestAmplificationInternal
import io.scalaland.chimney.dsl.*

/** Configures the submission request amplification. Amplification makes sequencer clients send
  * eligible submission requests to multiple sequencers to overcome message loss in faulty
  * sequencers.
  *
  * @param factor
  *   The maximum number of times the submission request shall be sent.
  * @param patience
  *   How long the sequencer client should wait after an acknowledged submission to a sequencer to
  *   observe the receipt or error before it attempts to send the submission request again (possibly
  *   to a different sequencer).
  */
final case class SubmissionRequestAmplification(
    factor: PositiveInt,
    patience: config.NonNegativeFiniteDuration,
) extends PrettyPrinting {
  override protected def pretty: Pretty[SubmissionRequestAmplification] = prettyOfClass(
    param("factor", _.factor),
    param("patience", _.patience),
  )

  def toInternal: SubmissionRequestAmplificationInternal =
    this.transformInto[SubmissionRequestAmplificationInternal]
}

object SubmissionRequestAmplification {
  val NoAmplification: SubmissionRequestAmplification =
    SubmissionRequestAmplification(PositiveInt.one, config.NonNegativeFiniteDuration.Zero)
}
