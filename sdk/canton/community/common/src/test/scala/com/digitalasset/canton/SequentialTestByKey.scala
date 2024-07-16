// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import org.scalatest.{BeforeAndAfterAll, Suite}

import java.util.concurrent.Semaphore
import scala.collection.concurrent.TrieMap

/** Our tests are typically run in parallel but sometimes we want to make sure specific tests are not run in parallel.
  * One example of this are tests that change the same DB table and it doesnt make sense to change the table just for the
  * sake of parallel testing ability.
  * By adding this mixin, semaphores will be used to only allow one test concurrently with the same semaphoreKey
  */
trait SequentialTestByKey extends BeforeAndAfterAll {
  self: Suite =>

  protected val semaphoreKey: Option[String]

  override def beforeAll(): Unit = {
    semaphoreKey.foreach(TestSemaphoreUtil.acquire)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    semaphoreKey.foreach(TestSemaphoreUtil.release)
  }
}

object TestSemaphoreUtil {
  private val semaphoreMap = TrieMap[String, Semaphore]()

  def acquire(key: String): Unit = {
    val sem = semaphoreMap.getOrElseUpdate(key, new Semaphore(1))
    sem.acquire()
  }

  def release(key: String): Unit =
    semaphoreMap.get(key).foreach(_.release())

  // pre-defined semaphore keys here
  val SEQUENCER_DB_H2 = Some("sequencer-db-h2")
  val SEQUENCER_DB_PG = Some("sequencer-db-pg")
  val SEQUENCER_DB_ORACLE = Some("sequencer-db-oracle")
}
