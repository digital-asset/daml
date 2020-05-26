// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.security.SecureRandom

import com.daml.lf.crypto

trait SeedService {
  def nextSeed: () => crypto.Hash
}

object SeedService {

  sealed abstract class Seeding

  object Seeding {
    case object Strong extends Seeding
    case object Weak extends Seeding
    case object Static extends Seeding
  }

  def apply(seeding: Seeding): SeedService =
    seeding match {
      case Seeding.Strong => StrongRandom
      case Seeding.Weak => WeakRandom
      case Seeding.Static => new StaticRandom("static random seed service")
    }

  // Pseudo random generator seeded with high entropy seed.
  // May block while gathering entropy from the underlying operating system
  // Thread safe.
  object StrongRandom extends SeedService {
    override val nextSeed: () => crypto.Hash =
      crypto.Hash.secureRandom(
        crypto.Hash.assertFromByteArray(
          SecureRandom.getInstanceStrong.generateSeed(crypto.Hash.underlyingHashLength)))
  }

  // Pseudo random generator seeded with low entropy seed.
  // Do not block. Thread safe.
  // Do not use in production mode.
  object WeakRandom extends SeedService {
    override val nextSeed: () => crypto.Hash = {
      val a = Array.ofDim[Byte](crypto.Hash.underlyingHashLength)
      scala.util.Random.nextBytes(a)
      crypto.Hash.secureRandom(crypto.Hash.assertFromByteArray(a))
    }
  }

  // Pseudo random generator seeded with a given seed.
  // Do not block. Thread safe.
  // Can be use to get reproducible run. Do not use in production mode.
  final class StaticRandom(seed: String) extends SeedService {
    override val nextSeed: () => crypto.Hash =
      crypto.Hash.secureRandom(crypto.Hash.hashPrivateKey(seed))
  }

}
