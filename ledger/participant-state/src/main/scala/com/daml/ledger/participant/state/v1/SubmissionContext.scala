// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

/** Collects context information in which the submission was processed
  * to detect and prevent race conditions if the context information changes during submission.
  *
  * @param ledgerConfiguration The ledger configuration used during interpretation
  */
case class SubmissionContext(
    ledgerConfiguration: Configuration
)
