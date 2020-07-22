// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

object SqlFunctions {
  def arrayIntersection(a: Array[String], b: Array[String]): Array[String] =
    varcharArraysIntersection(a, b)

  def varcharArraysIntersection(a: Array[String], b: Array[String]): Array[String] =
    a.toSet.intersect(b.toSet).toArray

  def doVarcharArraysIntersect(a: Array[String], b: Array[String]): Boolean =
    a.toSet.intersect(b.toSet).nonEmpty
}
