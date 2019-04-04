// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.akkastreams
import akka.NotUsed
import akka.stream.scaladsl.Source

trait HeadAwareDispatcher[Index, T] {

  /** Returns the head index where this Dispatcher is at */
  def getHead(): Index

  /** Signals and stores a new head. */
  def signalNewHead(head: Index): Unit

  /** Returns a stream of elements with the next index from start (inclusive) to end (exclusive) */
  def startingAt(start: Index, requestedEnd: Option[Index]): Source[(Index, T), NotUsed]
}
