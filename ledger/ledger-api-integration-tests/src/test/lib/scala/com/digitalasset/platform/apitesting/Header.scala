// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.time.{Duration, Instant}

object Header {
  private[this] val expiring = "Bearer ([\\w-]+) (\\d+)".r
  private[this] val unlimited = "Bearer ([\\w-]+)".r
  def unapply(arg: String): Option[(String, Option[Instant])] =
    arg match {
      case expiring(party, expiration) =>
        Some((party, Some(Instant.ofEpochMilli(expiration.toLong))))
      case unlimited(party) =>
        Some((party, None))
      case _ =>
        None
    }
  def apply(party: String): Header = new Header(party, None)
}

final case class Header private (party: String, expiration: Option[Instant]) {
  override val toString = s"Bearer $party${expiration.fold("")(t => s" ${t.toEpochMilli}")}"
  def expiresIn(t: java.time.Duration): Header = copy(expiration = Some(Instant.now.plus(t)))
  def expiresInOneSecond: Header = expiresIn(Duration.ofSeconds(1))
  def expiresTomorrow: Header = expiresIn(Duration.ofDays(1))
  def expired: Header = expiresIn(Duration.ofDays(-1))
}
