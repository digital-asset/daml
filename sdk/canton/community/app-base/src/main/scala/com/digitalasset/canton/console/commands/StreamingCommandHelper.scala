// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.config
import com.digitalasset.canton.console.CommandErrors.GenericCommandError
import com.digitalasset.canton.console.{CommandSuccessful, ConsoleEnvironment}
import com.digitalasset.canton.networking.grpc.{GrpcError, RecordingStreamObserver}
import com.digitalasset.canton.util.ResourceUtil
import io.grpc.StatusRuntimeException

import java.util.concurrent.TimeoutException
import scala.concurrent.Await

trait StreamingCommandHelper {
  protected def consoleEnvironment: ConsoleEnvironment
  protected def name: String

  def mkResult[Res](
      call: => AutoCloseable,
      requestDescription: String,
      observer: RecordingStreamObserver[Res],
      timeout: config.NonNegativeDuration,
  ): Seq[Res] = consoleEnvironment.run {
    try {
      ResourceUtil.withResource(call) { _ =>
        // Not doing noisyAwaitResult here, because we don't want to log warnings in case of a timeout.
        CommandSuccessful(
          Await.result(
            observer.result(consoleEnvironment.environment.executionContext),
            timeout.duration,
          )
        )
      }
    } catch {
      case sre: StatusRuntimeException =>
        GenericCommandError(GrpcError(requestDescription, name, sre).toString)
      case _: TimeoutException => CommandSuccessful(observer.responses)
    }
  }

}
