// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.command

import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.google.protobuf.empty.Empty

import scala.concurrent.{ExecutionContext, Future}

trait Tracker extends AutoCloseable {

  def track(request: SubmitAndWaitRequest)(implicit ec: ExecutionContext): Future[Empty]
}

object Tracker {

  class WithLastSubmission(delegate: Tracker) extends Tracker {

    override def close(): Unit = delegate.close()

    @volatile private var lastSubmission = System.nanoTime()

    def getLastSubmission: Long = lastSubmission

    override def track(request: SubmitAndWaitRequest)(
        implicit ec: ExecutionContext): Future[Empty] = {
      lastSubmission = System.nanoTime()
      delegate.track(request)
    }
  }

  object WithLastSubmission {
    def apply(delegate: Tracker): WithLastSubmission = new WithLastSubmission(delegate)
  }
}
