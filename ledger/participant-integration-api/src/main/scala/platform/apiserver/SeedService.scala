// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.security.SecureRandom
import java.util.concurrent.TimeUnit

import com.daml.lf.crypto

final class SeedService(seed: crypto.Hash) {
  val nextSeed: () => crypto.Hash = crypto.Hash.secureRandom(seed)
}

object SeedService {

  sealed abstract class Seeding(val name: String) extends Product with Serializable {
    override def toString: String = name
  }

  object Seeding {

    case object Strong extends Seeding("strong")

    case object Weak extends Seeding("testing-weak")

    case object Static extends Seeding("testing-static")

  }

  def apply(seeding: Seeding): SeedService =
    seeding match {
      case Seeding.Strong => StrongRandom
      case Seeding.Weak => WeakRandom
      case Seeding.Static => staticRandom("static random seed service")
    }

  /** Pseudo random generator seeded with high entropy seed.
    * May block while gathering entropy from the underlying operating system
    * Thread safe.
    */
  // lazy to avoid gathering unnecessary entropy.
  lazy val StrongRandom: SeedService = {
    val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
    val timer = new java.util.Timer()
    val task = new java.util.TimerTask {
      def run(): Unit = {
        logger.warn(
          """Trying to gather entropy from the underlying operating system to initialize the contract ID seeding, but the entropy pool seems empty."""
        )
        logger.warn(
          s"""In CI environments environment consider using the "${Seeding.Weak.name}" mode, that may produce insecure contract IDs but does not block on startup."""
        )
      }
    }
    timer.schedule(task, TimeUnit.SECONDS.toMillis(5))

    val seed = SecureRandom.getInstanceStrong.generateSeed(crypto.Hash.underlyingHashLength)

    timer.cancel()

    new SeedService(crypto.Hash.assertFromByteArray(seed))
  }

  /** Pseudo random generator seeded with a possibly low entropy seed.
    * Do not block. Thread safe.
    * Do not use in production mode.
    */
  lazy val WeakRandom: SeedService = {
    val seed = new Array[Byte](crypto.Hash.underlyingHashLength)
    // uses `nextBytes` on a default SecureRandom, that should not block
    new SecureRandom().nextBytes(seed)
    new SeedService(crypto.Hash.assertFromByteArray(seed))
  }

  /** Pseudo random generator seeded with a given seed.
    * Do not block. Thread safe.
    * Can be use to get reproducible run. Do not use in production mode.
    */
  def staticRandom(seed: String) = new SeedService(crypto.Hash.hashPrivateKey(seed))

}
