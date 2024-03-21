// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  final case class SErrorCrash(location: String, reason: String) extends SError {
    override def getMessage: String = s"CRASH ($location): $reason"
  }

  /** Daml exceptions that should be reported to the user. */
  final case class SErrorDamlException(error: interpretation.Error) extends SError

}
