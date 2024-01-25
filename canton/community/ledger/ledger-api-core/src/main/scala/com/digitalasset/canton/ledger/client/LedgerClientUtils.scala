// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client

import com.digitalasset.canton.error.DecodedRpcStatus
import com.google.rpc.code.Code
import com.google.rpc.status.Status

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object LedgerClientUtils {

  /** Default retry rules which will retry on retryable known errors and if the ledger api is unavailable */
  def defaultRetryRules: Status => Option[FiniteDuration] = status => {
    (DecodedRpcStatus.fromScalaStatus(status), status.code) match {
      case (Some(decoded), _) => decoded.retryIn
      case (None, Code.UNAVAILABLE.value | Code.DEADLINE_EXCEEDED.value) => Some(1.second)
      case (None, _) => None
    }
  }

  /** Convert codegen command to scala proto command */
  def javaCodegenToScalaProto(
      command: com.daml.ledger.javaapi.data.Command
  ): com.daml.ledger.api.v1.commands.Command = {
    com.daml.ledger.api.v1.commands.Command.fromJavaProto(command.toProtoCommand)
  }
}
