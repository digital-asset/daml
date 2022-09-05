// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.util.concurrent.CompletionStage

import com.daml.ledger.configuration.Configuration
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.telemetry.TelemetryContext

trait WriteConfigService {

  /** Submit a new configuration to the ledger. If the configuration is accepted
    * a [[Update.ConfigurationChanged]] event will be emitted to all participants.
    * In case of rejection a [[Update.ConfigurationChangeRejected]] will be emitted.
    *
    * The [[Configuration]] contains the identity of the participant that is allowed
    * to further change the configuration. The initial configuration can be submitted
    * by any participant.
    *
    * If configuration changes are not supported by the implementation then the
    * [[SubmissionResult.SynchronousError]] should be returned.
    * *
    * @param maxRecordTime: The maximum record time after which the request is rejected.
    * @param submissionId: Client picked submission identifier for matching the responses with the request.
    * @param config: The new ledger configuration.
    * @return an async result of a SubmissionResult
    */
  def submitConfiguration(
      maxRecordTime: Timestamp,
      submissionId: Ref.SubmissionId,
      config: Configuration,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult]
}
