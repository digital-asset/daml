// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.logging.pretty.Pretty

final case class WithRecipients[+A](private val x: A, recipients: Recipients) {
  def unwrap: A = x

  def map[B](f: A => B): WithRecipients[B] = copy(x = f(x))
}

object WithRecipients {
  implicit def prettyWithRecipients[A: Pretty]: Pretty[WithRecipients[A]] = {
    import Pretty.*
    prettyOfClass(
      unnamedParam(_.x),
      param("recipients", _.recipients),
    )
  }

}
