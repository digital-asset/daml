// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.util.{Success, Try}

private[daml] object TryOps {
  def sequence[A](list: List[Try[A]]): Try[List[A]] = {
    val zero: Try[List[A]] = Success(List.empty[A])
    list.foldRight(zero)((a, as) => map2(a, as)(_ :: _))
  }

  def map2[A, B, C](ta: Try[A], tb: Try[B])(f: (A, B) => C): Try[C] =
    for {
      a <- ta
      b <- tb
    } yield f(a, b)

}
