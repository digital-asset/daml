// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.syntax.option.*
import com.digitalasset.canton.admin.time.v30
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** @param initialRetryDelay The initial retry delay if the request to send a sequenced event fails
  * @param maxRetryDelay The max retry delay if the request to send a sequenced event fails
  * @param maxSequencingDelay If our request for a sequenced event was successful, how long should we wait
  *                                      to observe it from the sequencer before starting a new request.
  */
final case class TimeProofRequestConfig(
    initialRetryDelay: NonNegativeFiniteDuration = TimeProofRequestConfig.defaultInitialRetryDelay,
    maxRetryDelay: NonNegativeFiniteDuration = TimeProofRequestConfig.defaultMaxRetryDelay,
    maxSequencingDelay: NonNegativeFiniteDuration = TimeProofRequestConfig.defaultMaxSequencingDelay,
) extends PrettyPrinting {
  private[config] def toProtoV30: v30.TimeProofRequestConfig = v30.TimeProofRequestConfig(
    initialRetryDelay.toProtoPrimitive.some,
    maxRetryDelay.toProtoPrimitive.some,
    maxSequencingDelay.toProtoPrimitive.some,
  )
  override def pretty: Pretty[TimeProofRequestConfig] = prettyOfClass(
    paramIfNotDefault(
      "initialRetryDelay",
      _.initialRetryDelay,
      TimeProofRequestConfig.defaultInitialRetryDelay,
    ),
    paramIfNotDefault(
      "maxRetryDelay",
      _.maxRetryDelay,
      TimeProofRequestConfig.defaultMaxRetryDelay,
    ),
    paramIfNotDefault(
      "maxSequencingDelay",
      _.maxSequencingDelay,
      TimeProofRequestConfig.defaultMaxSequencingDelay,
    ),
  )

}

object TimeProofRequestConfig {

  private val defaultInitialRetryDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMillis(200)

  private val defaultMaxRetryDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(5)

  private val defaultMaxSequencingDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(10)

  private[config] def fromProtoV0(
      configP: v30.TimeProofRequestConfig
  ): ParsingResult[TimeProofRequestConfig] =
    for {
      initialRetryDelay <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("initialRetryDelay"),
        "initialRetryDelay",
        configP.initialRetryDelay,
      )
      maxRetryDelay <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("maxRetryDelay"),
        "maxRetryDelay",
        configP.maxRetryDelay,
      )
      maxSequencingDelay <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("maxSequencingDelay"),
        "maxSequencingDelay",
        configP.maxSequencingDelay,
      )
    } yield TimeProofRequestConfig(initialRetryDelay, maxRetryDelay, maxSequencingDelay)
}
