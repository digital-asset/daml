// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

/** A grouping of errors.
  *
  * @param docName The name that will appear in the generated documentation for the grouping.
  * @param group   If the grouping is defined by an [[ErrorGroup]], the associated instance.
  */
case class Grouping(
    docName: String,
    group: Option[ErrorGroup],
)

/** The classes [[ErrorClass]] and [[ErrorGroup]] are used to hierarchically structure error codes (their
  * hierarchical structure affects how they are displayed on the website)
  */
case class ErrorClass(groupings: List[Grouping]) {
  def extend(grouping: Grouping): ErrorClass =
    ErrorClass(groupings :+ grouping)
}

object ErrorClass {
  def root(): ErrorClass = ErrorClass(Nil)
}
