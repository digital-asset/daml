// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.crypto.Hash

trait SeedService {

  def nextSeed: () => crypto.Hash

}

object SeedService {

  sealed abstract class Seeding
  object Seeding {
    case object Weak extends Seeding
    case object Strong extends Seeding
  }

  def apply(seeding: Seeding): SeedService =
    seeding match {
      case Seeding.Strong =>
        new StrongRandomSeedService
      case Seeding.Weak =>
        new WeakRandomSeedService
    }

}

// This service uses a very low entropy seed.
// Do not use in production
class WeakRandomSeedService extends SeedService {
  override val nextSeed: () => Hash = {
    val weakSeed = Array.ofDim[Byte](32)
    scala.util.Random.nextBytes(weakSeed)
    crypto.Hash.random(weakSeed)
  }
}

// This service uses a PRNG with a high level entropy seed
class StrongRandomSeedService extends SeedService {
  override val nextSeed: () => Hash =
    crypto.Hash.secureRandom
}
