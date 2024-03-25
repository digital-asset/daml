// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

private[interning] final case class RawStringInterning(
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
        lastId = entries.view.foldLeft(rawStringInterning.lastId)((lastId, entry) =>
          Math.max(lastId, entry._1)
        ),
      )

  def newEntries(
      strings: Iterator[String],
      rawStringInterning: RawStringInterning,
  ): Vector[(Int, String)] =
    strings
      .filterNot(rawStringInterning.map.contains)
      .distinct
      .zipWithIndex
      .map { case (string, index) =>
        (index + 1 + rawStringInterning.lastId, string)
      }
      .toVector

  def resetTo(
      lastPersistedStringInterningId: Int,
      rawStringInterning: RawStringInterning,
  ): RawStringInterning =
    if (lastPersistedStringInterningId < rawStringInterning.lastId) {
      val idsToBeRemoved = lastPersistedStringInterningId + 1 to rawStringInterning.lastId
      val stringsToBeRemoved = idsToBeRemoved.map(rawStringInterning.idMap)

      RawStringInterning(
        map = rawStringInterning.map.removedAll(stringsToBeRemoved),
        idMap = rawStringInterning.idMap.removedAll(idsToBeRemoved),
        lastId = lastPersistedStringInterningId,
      )
    } else rawStringInterning
}
