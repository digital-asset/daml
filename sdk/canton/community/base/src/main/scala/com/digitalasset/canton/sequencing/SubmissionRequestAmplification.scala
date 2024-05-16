// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.admin.domain.v30
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** Configures the submission request amplification.
  * Amplification makes sequencer clients send eligible submission requests to multiple sequencers
  * to overcome message loss in faulty sequencers.
  *
  * @param factor The maximum number of times the submission request shall be sent.
  * @param patience How long the sequencer client should wait after an acknowledged submission to a sequencer
  *                 to observe the receipt or error before it attempts to send the submission request again
  *                 (possibly to a different sequencer).
  */
final case class SubmissionRequestAmplification(
    factor: PositiveInt,
    patience: config.NonNegativeFiniteDuration,
) extends PrettyPrinting {

  private[sequencing] def toProtoV30: v30.SubmissionRequestAmplification =
    v30.SubmissionRequestAmplification(
      factor = factor.unwrap,
      patience = Some(patience.toProtoPrimitive),
    )

  override def pretty: Pretty[SubmissionRequestAmplification] = prettyOfClass(
    param("factor", _.factor),
    param("patience", _.patience),
  )
}

object SubmissionRequestAmplification {
  val NoAmplification: SubmissionRequestAmplification =
    SubmissionRequestAmplification(PositiveInt.one, config.NonNegativeFiniteDuration.Zero)

  private[sequencing] def fromProtoV30(
      proto: v30.SubmissionRequestAmplification
  ): ParsingResult[SubmissionRequestAmplification] = {
    val v30.SubmissionRequestAmplification(factorP, patienceP) = proto
    for {
      factor <- ProtoConverter.parsePositiveInt(factorP)
      patience <- ProtoConverter.parseRequired(
        config.NonNegativeFiniteDuration.fromProtoPrimitive("patience"),
        "patience",
        patienceP,
      )
    } yield SubmissionRequestAmplification(factor, patience)
  }
}
