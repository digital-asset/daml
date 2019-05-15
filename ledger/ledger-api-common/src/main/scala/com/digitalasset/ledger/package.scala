package com.digitalasset

import com.digitalasset.daml.lf.data.Ref

package object ledger {

  /** Identifiers used to correlate submission with results.
    * Currently unrestricted unicode (See issue #398). */
  val CommandId: Ref.LedgerName.type = Ref.LedgerName
  type CommandId = CommandId.T

  /** Identifiers used for correlating submission with a workflow.
    * Currently unrestricted unicode (See issue #398).  */
  val WorkflowId: Ref.LedgerName.type = Ref.LedgerName
  type WorkflowId = WorkflowId.T

  /** Identifiers for submitting client applications.
    * Currently unrestricted unicode (See issue #398). */
  val ApplicationId: Ref.LedgerName.type = Ref.LedgerName
  type ApplicationId = ApplicationId.T

}
