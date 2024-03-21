// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.ledger.api.v1.transaction.Transaction
import com.digitalasset.canton.tracing.TraceContext

/** Admin service that connects to the ledger and process events
  */
trait AdminWorkflowService extends AutoCloseable {

  /** Processing the transaction must not block; shutdown problems occur otherwise.
    * Long-running computations or blocking calls should be spawned off into an asynchronous computation
    * so that the service itself can synchronize its closing with the spawned-off computation if needed.
    */
  private[admin] def processTransaction(tx: Transaction)(implicit traceContext: TraceContext): Unit
}
