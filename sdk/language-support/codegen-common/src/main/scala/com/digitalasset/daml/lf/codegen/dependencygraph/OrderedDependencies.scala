// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.dependencygraph

import com.digitalasset.daml.lf.codegen.exception.UnsupportedTypeError

/** Ordered dependencies where the dependant node always comes after the dependency.
  *
  * @param deps The ordered dependencies
  * @param errors The errors that came up when producing the list
  */
final case class OrderedDependencies[+K, +A](
    deps: Vector[(K, Node[K, A])],
    errors: List[UnsupportedTypeError],
)
