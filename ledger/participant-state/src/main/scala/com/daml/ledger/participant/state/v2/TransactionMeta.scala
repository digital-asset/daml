// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.time.Instant

/** Meta-data of a transaction visible to all parties that can see a part of
  * the transaction.
  *
  * @param ledgerEffectiveTime: the submitter-provided time at which the
  *   transaction should be interpreted. This is the time returned by the
  *   DAML interpreter on a `getTime :: Update Time` call. See the docs on
  *   [[WriteService.submitTransaction]] for how it relates to the notion of
  *   `recordTime`.
  *
  * @param workflowId: a submitter-provided identifier used for monitoring
  *   and to traffic-shape the work handled by DAML applications
  *   communicating over the ledger.
  *
  */
final case class TransactionMeta(ledgerEffectiveTime: Instant, workflowId: Option[WorkflowId])
