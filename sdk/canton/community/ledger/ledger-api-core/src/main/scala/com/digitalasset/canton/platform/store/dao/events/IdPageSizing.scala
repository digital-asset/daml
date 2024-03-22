// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext

/** The size of a page is the number of ids in the page.
  */
final case class IdPageSizing(
    minPageSize: Int,
    maxPageSize: Int,
) {
  assert(minPageSize > 0)
  assert(maxPageSize >= minPageSize)
}

object IdPageSizing {

  // Approximation of how many index entries is present in a leaf page of a btree index.
  // Fetching fewer ids than this only adds round-trip overhead, without decreasing the number of disk page reads per round-trip.
  // Experiments, with default fill ratio for BTree Index, show  that:
  // - (party_id, template_id) index has 244 tuples per disk page,
  // - wildcard party_id index has 254 per disk page.
  // We are picking a smaller number to accommodate for pruning, deletions and index bloat effect
  // which all result in a smaller ratio of tuples per disk page.
  private val NumOfBtreeLeafPageEntriesApprox = 200

  /** Calculates the ideal page sizes to fetch ids with.
    */
  def calculateFrom(
      maxIdPageSize: Int,
      workingMemoryInBytesForIdPages: Int,
      numOfDecomposedFilters: Int,
      numOfPagesInIdPageBuffer: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext): IdPageSizing = {
    val logger = loggerFactory.getTracedLogger(getClass)
    val calculated = calculateMaxNumOfIdsPerPage(
      workingMemoryInBytesForIdPages = workingMemoryInBytesForIdPages,
      numOfDecomposedFilters = numOfDecomposedFilters,
      numOfPagesInIdPageBuffer = numOfPagesInIdPageBuffer,
    )
    // maxNumberOfIdsPerIdPage can override this if it is smaller
    val minIdPageSize = Math.min(10, maxIdPageSize)
    // maxNumberOfIdsPerIdPage can override this if it is smaller
    val recommendedIdPageSize = Math.min(NumOfBtreeLeafPageEntriesApprox, maxIdPageSize)
    if (calculated < minIdPageSize) {
      logger.warn(
        s"Calculated maximum ID page size supporting API stream memory limits [$calculated] is too low: $minIdPageSize is used instead. " +
          s"Warning: API stream memory limits not respected. Warning: Dangerously low maximum ID page size can cause poor streaming performance. " +
          s"Filter size [$numOfDecomposedFilters] too large?"
      )
      IdPageSizing(minIdPageSize, minIdPageSize)
    } else if (calculated < recommendedIdPageSize) {
      logger.warn(
        s"Calculated maximum ID page size supporting API stream memory limits [$calculated] is very low. " +
          s"Warning: Low maximum ID page size can cause poor streaming performance. Filter size [$numOfDecomposedFilters] too large?"
      )
      IdPageSizing(calculated, calculated)
    } else if (calculated < maxIdPageSize) {
      logger.info(
        s"Calculated maximum ID page size supporting API stream memory limits [$calculated] is low. " +
          s"Warning: Low maximum ID page size can cause poor streaming performance. Filter size [$numOfDecomposedFilters] too large?"
      )
      IdPageSizing(recommendedIdPageSize, calculated)
    } else {
      logger.debug(
        s"Calculated maximum ID page size supporting API stream memory limits [$calculated] is high, using [$maxIdPageSize] instead."
      )
      IdPageSizing(recommendedIdPageSize, maxIdPageSize)
    }
  }

  private def calculateMaxNumOfIdsPerPage(
      workingMemoryInBytesForIdPages: Int,
      numOfDecomposedFilters: Int,
      numOfPagesInIdPageBuffer: Int,
  ): Int = {
    // An id occupies 8 bytes (it's a 64-bit long)
    val numOfIdsInMemory = workingMemoryInBytesForIdPages / 8
    // For each decomposed filter we have:
    //  1) one page fetched for merge sorting
    //  2) and additional pages residing in the buffer.
    val maxNumOfIdPages = (numOfPagesInIdPageBuffer + 1) * numOfDecomposedFilters
    val maxNumOfIdsPerPage = numOfIdsInMemory / maxNumOfIdPages
    maxNumOfIdsPerPage
  }
}
