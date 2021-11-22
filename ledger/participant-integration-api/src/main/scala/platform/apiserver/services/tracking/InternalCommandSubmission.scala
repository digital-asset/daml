package com.daml.platform.apiserver.services.tracking

import java.time.Duration

import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.client.services.commands.tracker.{Trackable, TrackerCommandInput}

case class InternalCommandSubmission(request: SubmitRequest, timeout: Option[Duration] = None)

object InternalCommandSubmission {
  implicit val trackable: Trackable[InternalCommandSubmission] =
    (value: InternalCommandSubmission) =>
      TrackerCommandInput(
        value.request.commands,
        value.timeout,
      )
}
