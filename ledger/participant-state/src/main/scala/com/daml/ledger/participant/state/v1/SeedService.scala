// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.security.SecureRandom
import java.util.concurrent.TimeUnit.SECONDS
import java.util.{Timer, TimerTask}

import com.daml.lf.crypto
import org.slf4j.LoggerFactory

final class SeedService(seed: crypto.Hash) {
  def nextSeed: () => crypto.Hash = crypto.Hash.secureRandom(seed)
}

object SeedService {

  sealed abstract class Seeding(val name: String) extends Product with Serializable {
    override def toString: String = name
  }

  object Seeding {

    val NoSeedingModeName = "no"

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
  lazy val StrongRandom = {
    val logger = LoggerFactory.getLogger(this.getClass)
    val timer = new Timer()
    def sndTask = new TimerTask {
      def run(): Unit =
        logger.info(
          s""""Still Trying to gather entropy to initialized the contract ID seeding...  """"
        )
    }
    val fstTask = new TimerTask {
      def run(): Unit = {
        logger.info(
          s"""Trying to gather entropy from the underlying operating system to initialized the contract ID seeding, but the entropy pool seems empty.
             |On testing environment consider using the "${Seeding.Weak.name}" mode, that produces insecure contract IDs but does not block on startup.""".stripMargin
        )
        timer.schedule(sndTask, SECONDS.toMillis(10), SECONDS.toMillis(10))
      }
    }
    timer.schedule(fstTask, SECONDS.toMillis(5))

    val seed = SecureRandom.getInstanceStrong.generateSeed(crypto.Hash.underlyingHashLength)

    timer.cancel()

    new SeedService(crypto.Hash.assertFromByteArray(seed))
  }

  /** Pseudo random generator seeded with low entropy seed.
    * Do not block. Thread safe.
    * Do not use in production mode.
    */
  val WeakRandom: SeedService = {
    val seed = new SecureRandom().generateSeed(crypto.Hash.underlyingHashLength)
    new SeedService(crypto.Hash.assertFromByteArray(seed))
  }

  /** Pseudo random generator seeded with a given seed.
    * Do not block. Thread safe.
    * Can be use to get reproducible run. Do not use in production mode.
    */
  def staticRandom(seed: String) = new SeedService(crypto.Hash.hashPrivateKey(seed))

}
