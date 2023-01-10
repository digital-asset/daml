// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

sealed trait UpdatePathError {
  def getReason: String = getClass.getSimpleName
}

object UpdatePathError {

  private def shorten(s: String): String =
    if (s.length > 53) { s.take(50) + "..." }
    else s

  final case class UnknownFieldPath(rawUpdatePath: String) extends UpdatePathError {
    override def getReason: String =
      s"The update path: '${shorten(rawUpdatePath)}' points to an unknown field."
  }

  final case class DuplicatedFieldPath(rawUpdatePath: String) extends UpdatePathError {
    override def getReason: String =
      s"The update path: '${shorten(rawUpdatePath)}' is duplicated."
  }

  final case object EmptyUpdateMask extends UpdatePathError {
    override def getReason: String = "The update mask contains no entries"
  }
}
