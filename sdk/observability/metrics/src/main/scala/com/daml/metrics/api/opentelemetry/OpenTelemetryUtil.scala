// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.opentelemetry

import io.opentelemetry.sdk.common.CompletableResultCode

import scala.concurrent.{Future, Promise}

object OpenTelemetryUtil {

  def completionCodeToFuture(completion: CompletableResultCode): Future[Unit] = {
    val promise = Promise[Unit]()
    completion.whenComplete(() => promise.success(()))
    promise.future
  }

}
