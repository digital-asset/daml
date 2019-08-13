// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.refinements

import scalaz.{@@, Tag}

import scala.util.Random

class IdGenerator[TypeTag](seed: Long) {

  private val rand = new Random(seed)

  def generateRandom: String @@ TypeTag = Tag.apply(rand.nextLong().toHexString)

}
