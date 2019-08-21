// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.util.concurrent.CompletionStage

import com.digitalasset.daml.lf.data.Time.Timestamp

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
    * [[SubmissionResult.NotSupported]] should be returned.
    **
    * @param maxRecordTime: The maximum record time after which the request is rejected.
    * @param submissionId: Client picked submission identifier for matching the responses with the request.
    * @param config: The new ledger configuration.
    * @return an async result of a SubmissionResult
    */
  def submitConfiguration(
      maxRecordTime: Timestamp,
      submissionId: String,
      config: Configuration
  ): CompletionStage[SubmissionResult]
}
