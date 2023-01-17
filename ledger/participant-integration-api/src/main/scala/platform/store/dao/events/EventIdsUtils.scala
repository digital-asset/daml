// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.annotation.tailrec

object EventIdsUtils {

  def mergeSortAndDeduplicateIds(ids: Vector[Source[Long, NotUsed]]): Source[Long, NotUsed] =
    mergeSortAndDeduplicateIds(ids).statefulMapConcat(statefulDeduplicate)

  @tailrec
  protected[events] def mergeSort[T: Ordering](
      sources: Vector[Source[T, NotUsed]]
  ): Source[T, NotUsed] =
    if (sources.isEmpty) Source.empty
    else if (sources.size == 1) sources.head
    else
      mergeSort(
        sources
          .drop(2)
          .appended(
            sources.take(2).reduce(_ mergeSorted _)
          )
      )

  protected[events] def statefulDeduplicate[T]: () => T => List[T] =
    () => {
      var last = null.asInstanceOf[T]
      elem =>
        if (elem == last) Nil
        else {
          last = elem
          List(elem)
        }
    }

}
