// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.v2

import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.CompletionStage

trait WriteConfigService {

  /** Submit a new configuration to the ledger. If the configuration is accepted
    * a [[com.digitalasset.canton.ledger.participant.state.v2.Update.ConfigurationChanged]] event will be emitted to all participants.
    * In case of rejection a [[com.digitalasset.canton.ledger.participant.state.v2.Update.ConfigurationChangeRejected]] will be emitted.
    *
    * The [[com.digitalasset.canton.ledger.configuration.Configuration]] contains the identity of the participant that is allowed
    * to further change the configuration. The initial configuration can be submitted
    * by any participant.
    *
    * If configuration changes are not supported by the implementation then the
    * [[com.digitalasset.canton.ledger.participant.state.v2.SubmissionResult.SynchronousError]] should be returned.
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
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult]
}
