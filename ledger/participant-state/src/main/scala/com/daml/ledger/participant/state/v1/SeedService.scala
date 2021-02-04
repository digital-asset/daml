// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.security.SecureRandom

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
      case Seeding.Static => StaticRandom
    }

  /** Pseudo random generator seeded with high entropy seed.
    * May block while gathering entropy from the underlying operating system
    * Thread safe.
    */
  // lazy to avoid gathering unecessary entropy.
  lazy val StrongRandom = {
    val logger = LoggerFactory.getLogger(this.getClass)
    var done = false

    new Thread {
      override def run(): Unit = {
        Thread.sleep(5 * 1000)
        if (!done) {
          logger.info(
            s"""Trying to gather entropy from the underlying operating system to initialized the contract ID seeding, but the entropy pool seems empty.
               |On testing environment consider using the "${Seeding.Weak.name}" mode, that does not block on startup but produces insecure contract IDs.""".stripMargin
          )
        }
        while (!done) {
          Thread.sleep(10 * 1000)
          logger.info("Still Trying to gather entropy to initialized the contract ID seeding...  ")
        }
      }
    }.start()

    val seed = SecureRandom.getInstanceStrong.generateSeed(crypto.Hash.underlyingHashLength)

    done = true

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

  /** Pseudo random generator seeded with a static seed.
    * Do not block. Thread safe.
    * Can be use to get reproducible run. Do not use in production mode.
    */
  val StaticRandom = staticRandom("static random seed service")

  /** Pseudo random generator seeded with a given seed.
    * Do not block. Thread safe.
    * Can be use to get reproducible run. Do not use in production mode.
    */
  def staticRandom(seed: String) = new SeedService(crypto.Hash.hashPrivateKey(seed))

}
