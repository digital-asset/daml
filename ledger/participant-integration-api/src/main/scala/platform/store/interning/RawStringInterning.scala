// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.interning

private[interning] case class RawStringInterning(
    map: Map[String, Int],
    idMap: Map[Int, String],
    lastId: Int,
)

private[interning] object RawStringInterning {

  def from(
      entries: Iterable[(Int, String)],
      rawStringInterning: RawStringInterning = RawStringInterning(Map.empty, Map.empty, 0),
  ): RawStringInterning =
    if (entries.isEmpty) rawStringInterning
    else
      RawStringInterning(
        map = rawStringInterning.map ++ entries.view.map(_.swap),
        idMap = rawStringInterning.idMap ++ entries,
        lastId = entries.view.map(_._1).max,
      )

  def newEntries(
      strings: Iterator[String],
      rawStringInterning: RawStringInterning,
  ): Vector[(Int, String)] =
    strings
      .filterNot(rawStringInterning.map.contains)
      .toVector
      .distinct // TODO Iterators do not have .distinct in Scala 2.12
      .view
      .zipWithIndex
      .map { case (string, index) =>
        (index + 1 + rawStringInterning.lastId, string)
      }
      .toVector
}
