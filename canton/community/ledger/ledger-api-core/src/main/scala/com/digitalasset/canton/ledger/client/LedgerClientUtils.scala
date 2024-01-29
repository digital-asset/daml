// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client

import com.daml.error.utils.DecodedCantonError
import com.google.rpc.code.Code
import com.google.rpc.status.Status

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object LedgerClientUtils {

  /** Default retry rules which will retry on retryable known errors and if the ledger api is unavailable */
  def defaultRetryRules: Status => Option[FiniteDuration] = status =>
    DecodedCantonError
      .fromGrpcStatus(status)
      .toOption
      .flatMap(_.retryIn)
      .orElse {
        Option.when(
          status.code == Code.UNAVAILABLE.value || status.code == Code.DEADLINE_EXCEEDED.value
        )(1.second)
      }

  /** Convert codegen command to scala proto command */
  def javaCodegenToScalaProto(
      command: com.daml.ledger.javaapi.data.Command
  ): com.daml.ledger.api.v1.commands.Command =
    com.daml.ledger.api.v1.commands.Command.fromJavaProto(command.toProtoCommand)
}
