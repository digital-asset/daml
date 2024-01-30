// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.ledger.api.v2.state_service.ActiveContract
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.digitalasset.canton.tracing.TraceContext

/** Admin service that connects to the ledger and process events
  */
trait AdminWorkflowService extends AutoCloseable {

  /** The filters for this service */
  private[admin] def filters: TransactionFilter

  /** Processing the transaction must not block; shutdown problems occur otherwise.
    * Long-running computations or blocking calls should be spawned off into an asynchronous computation
    * so that the service itself can synchronize its closing with the spawned-off computation if needed.
    */
  private[admin] def processTransaction(tx: Transaction): Unit

  /** Process a reassignment */
  private[admin] def processReassignment(tx: Reassignment): Unit

  /** Process the initial active contract set for this service */
  private[admin] def processAcs(acs: Seq[ActiveContract])(implicit
      traceContext: TraceContext
  ): Unit

}
