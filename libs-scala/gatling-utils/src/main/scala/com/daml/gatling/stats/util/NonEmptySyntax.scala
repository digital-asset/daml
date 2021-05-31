// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats.util

object NonEmptySyntax {
  implicit class NonEmptyOps[A](val seq: Seq[A]) extends AnyVal {
    def nonEmptyOpt: Option[Seq[A]] = if (seq.isEmpty) None else Some(seq)
  }
}
