// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

/** A component of [[ErrorClass]]
  *
  * @param docName The name that will appear in the generated documentation for the grouping.
  * @param fullClassName Full class name of the corresponding [[ErrorGroup]].
  */
case class Grouping(
    docName: String,
    fullClassName: String,
) {
  require(
    docName.trim.nonEmpty,
    s"Grouping.docName must be non empty and must contain not only whitespace characters, but was: |${docName}|!",
  )
}

/** Used to hierarchically structure error codes in the official documentation.
  */
case class ErrorClass(groupings: List[Grouping]) {
  def extend(grouping: Grouping): ErrorClass =
    ErrorClass(groupings :+ grouping)
}

object ErrorClass {
  def root(): ErrorClass = ErrorClass(Nil)
}
