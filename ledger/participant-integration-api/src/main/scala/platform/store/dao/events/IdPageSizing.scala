// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.logging.{ContextualizedLogger, LoggingContext}

/** The size of a page is the number of ids in the page.
  */
case class IdPageSizing(
    minPageSize: Int,
    maxPageSize: Int,
) {
  assert(minPageSize > 0)
  assert(maxPageSize >= minPageSize)
}

object IdPageSizing {
  private val logger = ContextualizedLogger.get(getClass)

  def calculateFrom(
      maxIdPageSize: Int,
      idPageWorkingMemoryBytes: Int,
      filterSize: Int,
      idPageBufferSize: Int,
  )(implicit loggingContext: LoggingContext): IdPageSizing = {
    val LowestIdPageSize =
      Math.min(10, maxIdPageSize) // maxIdPageSize can override this if it is smaller
    // Approximation how many index entries can be present in one btree index leaf page (fetching smaller than this only adds round-trip overhead, without boiling down to smaller disk read)
    // Experiments show party_id, template_id index has 244 tuples per page, wildcard party_id index has 254 per page (with default fill ratio for BTREE Index)
    // Picking a lower number is for accommodating pruning, deletions, index bloat effect, which boil down to lower tuple per page ratio.
    val RecommendedMinIdPageSize =
      Math.min(200, maxIdPageSize) // maxIdPageSize can override this if it is smaller
    val calculatedMaxIdPageSize =
      idPageWorkingMemoryBytes
        ./(8) // IDs stored in 8 bytes
        ./(
          idPageBufferSize + 1
        ) // for each filter we need one page fetched for merge sorting, and additional pages might reside in the buffer
        ./(filterSize)
    if (calculatedMaxIdPageSize < LowestIdPageSize) {
      logger.warn(
        s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxIdPageSize] is too low: $LowestIdPageSize is used instead. Warning: API stream memory limits not respected. Warning: Dangerously low maximum ID page size can cause poor streaming performance. Filter size [$filterSize] too large?"
      )
      IdPageSizing(LowestIdPageSize, LowestIdPageSize)
    } else if (calculatedMaxIdPageSize < RecommendedMinIdPageSize) {
      logger.warn(
        s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxIdPageSize] is very low. Warning: Low maximum ID page size can cause poor streaming performance. Filter size [$filterSize] too large?"
      )
      IdPageSizing(calculatedMaxIdPageSize, calculatedMaxIdPageSize)
    } else if (calculatedMaxIdPageSize < maxIdPageSize) {
      logger.info(
        s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxIdPageSize] is low. Warning: Low maximum ID page size can cause poor streaming performance. Filter size [$filterSize] too large?"
      )
      IdPageSizing(RecommendedMinIdPageSize, calculatedMaxIdPageSize)
    } else {
      logger.debug(
        s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxIdPageSize] is sufficiently high, using [$maxIdPageSize] instead."
      )
      IdPageSizing(RecommendedMinIdPageSize, maxIdPageSize)
    }
  }
}
