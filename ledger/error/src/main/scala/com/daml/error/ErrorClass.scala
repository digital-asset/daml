// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

/** The classes [[ErrorClass]] and [[ErrorGroup]] are used to hierarchically structure error codes (their
  * hierarchical structure affects how they are displayed on the website)
  */
case class ErrorClass(docNames: List[String]) {
  def extend(docName: String = ""): ErrorClass = {
    ErrorClass(docNames :+ docName)
  }
}
object ErrorClass {
  def root(): ErrorClass = ErrorClass(Nil)
}
