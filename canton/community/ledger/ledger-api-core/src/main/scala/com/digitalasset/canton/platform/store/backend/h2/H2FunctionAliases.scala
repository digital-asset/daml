// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

// Warning this object is accessed directly by H2Database, see "CREATE ALIAS" in H2 related FLyway scripts
object H2FunctionAliases {

  def arrayIntersection(
      a: Array[java.lang.Integer],
      b: Array[java.lang.Integer],
  ): Array[java.lang.Integer] =
    a.toSet.intersect(b.toSet).toArray

}
