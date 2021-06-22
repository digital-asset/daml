// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import scala.util.control.NoStackTrace

object SError {

  /** Errors that can arise during interpretation */
  sealed abstract class SError
      extends RuntimeException
      with NoStackTrace
      with Product
      with Serializable

  /** A malformed expression was encountered. The assumption is that the
    * expressions are type-checked and the loaded packages have been validated,
    * hence we do not have separate errors for e.g. unknown values.
    */
  final case class SErrorCrash(reason: String) extends SError {
    override def toString = "CRASH: " + reason
  }

  /** Operation is only supported in on-ledger mode but was
    * called in off-ledger mode.
    */
  final case class SRequiresOnLedger(operation: String) extends SError {
    override def toString = s"Requires on-ledger mode: " + operation
  }

  def crash[A](reason: String): A =
    throw SErrorCrash(reason)

  /** Daml exceptions that should be reported to the user. */
  final case class SErrorDamlException(error: interpretation.Error) extends SError

}
