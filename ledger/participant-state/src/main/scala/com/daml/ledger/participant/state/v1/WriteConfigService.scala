// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.util.concurrent.CompletionStage

import com.digitalasset.daml.lf.data.Time.Timestamp

trait WriteConfigService {

  /** Submit a new configuration to the ledger. If the configuration is accepted
    * a [[Update.ConfigurationChanged]] event will be emitted to all participants.
    *
    * The [[Configuration]] contains the identity of the participant that is allowed
    * to further change the configuration. The initial configuration can be submitted
    * by any participant.
    *
    * Rejected configuration submissions are not evidenced. The caller can retry
    * the request after the previous 'maxRecordTime' has expired.
    *
    * @param maxRecordTime: The maximum record time after which the request is rejected.
    * @param currentConfig: The current ledger configuration that is being replaced. Request is rejected if the configuration does not match.
    * @param newConfig: The new ledger configuration.
    * @return an async result of a SubmissionResult
    */
  def submitConfiguration(
      maxRecordTime: Timestamp,
      currentConfig: Configuration,
      newConfig: Configuration
  ): CompletionStage[SubmissionResult]
}
