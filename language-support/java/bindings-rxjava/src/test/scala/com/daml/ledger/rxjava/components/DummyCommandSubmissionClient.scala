// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components

import java.time.Instant
import java.util

import com.daml.ledger.rxjava.CommandSubmissionClient
import com.daml.ledger.javaapi.data.{Command, SubmitCommandsRequest}
import com.google.protobuf.Empty
import io.reactivex.Single

class DummyCommandSubmissionClient(f: SubmitCommandsRequest => Option[Throwable])
    extends CommandSubmissionClient {
  override def submit(
      workflowId: String,
      applicationId: String,
      commandId: String,
      party: String,
      ledgerEffectiveTime: Instant,
      maximumRecordTime: Instant,
      commands: util.List[Command]): Single[Empty] =
    f(
      new SubmitCommandsRequest(
        workflowId,
        applicationId,
        commandId,
        party,
        ledgerEffectiveTime,
        maximumRecordTime,
        commands))
      .fold(Single.just(Empty.getDefaultInstance))(t => Single.error(t))
}
