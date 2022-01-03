// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

import scala.annotation.tailrec

// Trampolines for stack-safety
private[lf] object Trampoline {

  private[lf] sealed trait Trampoline[A] {

    @tailrec
    final def bounce: A = this match {
      case Land(a) => a
      case Bounce(continue) => continue().bounce
    }

  }

  final case class Land[A](a: A) extends Trampoline[A]
  final case class Bounce[A](func: () => Trampoline[A]) extends Trampoline[A]

}
