// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.digitalasset.canton.discard.Implicits.DiscardOps

import scala.collection.mutable

/** Keeps track of contract keys of contracts that haven't been used up (archived) yet. Allows to
  * select the next contract key to use up at random.
  */
final class ActiveContractKeysPool[T](randomnessProvider: RandomnessProvider) {

  private val poolsPerTemplate = mutable.Map.empty[String, DepletingUniformRandomPool[T]]

  def getAndRemoveContractKey(templateName: String): T = synchronized {
    val pool = poolsPerTemplate(templateName)
    pool.pop()
  }

  def addContractKey(templateName: String, value: T): Unit = synchronized {
    if (!poolsPerTemplate.contains(templateName)) {
      poolsPerTemplate.put(templateName, new DepletingUniformRandomPool(randomnessProvider)).discard
    }
    val pool = poolsPerTemplate(templateName)
    pool.put(value)
  }
}

/** A pool of elements supporting two operations:
  *   1. pop() - select an element uniformly at random and remove it from the pool.
  *   1. put() - add an element to the pool
  */
final class DepletingUniformRandomPool[V](randomnessProvider: RandomnessProvider) {
  private val buffer = mutable.ArrayBuffer.empty[V]

  def pop(): V = {
    val v = buffer.last
    buffer.remove(index = buffer.size - 1, count = 1)
    v
  }

  def put(v: V): Unit = {
    val i = randomnessProvider.randomNatural(buffer.size + 1)
    buffer.insert(index = i, elem = v)
  }
}
