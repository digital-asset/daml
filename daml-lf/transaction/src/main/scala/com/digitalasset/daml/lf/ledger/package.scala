package com.daml.lf

package object ledger {

  type FailedAuthorizations = Map[transaction.Transaction.NodeId, FailedAuthorization]

}
