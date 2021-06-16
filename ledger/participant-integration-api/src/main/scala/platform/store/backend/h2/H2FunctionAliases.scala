// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

// Warning this object is accessed directly by H2Database, see "CREATE ALIAS" in H2 related FLyway scripts
object H2FunctionAliases {

  def arrayIntersection(a: Array[String], b: Array[String]): Array[String] =
    a.toSet.intersect(b.toSet).toArray

}
