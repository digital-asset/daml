package com.digitalasset

import com.digitalasset.daml.lf.data.Ref

package object ledger {

  /** Identifiers used to correlate submission with results.
    * Currently unrestricted unicode (See issue #398). */
  val CommandId: Ref.LedgerString.type = Ref.LedgerString
  type CommandId = CommandId.T

  /** Identifiers used for correlating submission with a workflow.
    * Currently unrestricted unicode (See issue #398).  */
  val WorkflowId: Ref.LedgerString.type = Ref.LedgerString
  type WorkflowId = WorkflowId.T

  /** Identifiers for submitting client applications.
    * Currently unrestricted unicode (See issue #398). */
  val ApplicationId: Ref.LedgerString.type = Ref.LedgerString
  type ApplicationId = ApplicationId.T

}
