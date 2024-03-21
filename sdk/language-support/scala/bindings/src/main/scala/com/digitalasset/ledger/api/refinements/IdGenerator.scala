// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.refinements

import scalaz.{@@, Tag}

import scala.util.Random

class IdGenerator[TypeTag](seed: Long) {

  private val rand = new Random(seed)

  def generateRandom: String @@ TypeTag = Tag.apply(rand.nextLong().toHexString)

}
