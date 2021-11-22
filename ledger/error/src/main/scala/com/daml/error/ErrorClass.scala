// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    s"ErrorGroupSegment.docName must be non mmpty and must contain not only whitespace characters, but was: |${docName}|!",
  )
}

/** Used to hierarchically structure error codes in the official documentation.
  */
case class ErrorClass(segments: List[Grouping]) {
  def extend(last: Grouping): ErrorClass =
    ErrorClass(segments :+ last)
}

object ErrorClass {
  def root(): ErrorClass = ErrorClass(Nil)
}
