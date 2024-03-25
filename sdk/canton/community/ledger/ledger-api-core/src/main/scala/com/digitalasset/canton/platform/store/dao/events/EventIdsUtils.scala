// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.annotation.tailrec

object EventIdsUtils {

  def sortAndDeduplicateIds(ids: Vector[Source[Long, NotUsed]]): Source[Long, NotUsed] =
    mergeSort(ids).statefulMapConcat(statefulDeduplicate)

  @tailrec
  protected[events] def mergeSort[T: Ordering](
      sources: Vector[Source[T, NotUsed]]
  ): Source[T, NotUsed] = {
    sources match {
      case Vector(first, second, _*) =>
        mergeSort(
          sources
            .drop(2)
            .appended(first.mergeSorted(second))
        )
      case Vector(head) => head
      case _ => Source.empty
    }
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Null",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Var",
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
