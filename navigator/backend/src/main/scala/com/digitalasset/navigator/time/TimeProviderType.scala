// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.time

import scopt.Read

sealed abstract class TimeProviderType extends Product with Serializable {
  def name: String
}
object TimeProviderType {

  case object Auto extends TimeProviderType {
    val name = "auto"
  }
  case object Static extends TimeProviderType {
    val name = "static"
  }
  case object Simulated extends TimeProviderType {
    val name = "simulated"
  }
  case object WallClock extends TimeProviderType {
    val name = "wallclock"
  }

  def unapply(s: String): Option[TimeProviderType] =
    if (s.compareToIgnoreCase(Auto.name) == 0) Some(Auto)
    else if (s.compareToIgnoreCase(Static.name) == 0) Some(Static)
    else if (s.compareToIgnoreCase(Simulated.name) == 0) Some(Simulated)
    else if (s.compareToIgnoreCase(WallClock.name) == 0) Some(WallClock)
    else None

  def write(tpt: TimeProviderType): String = tpt.name

  def acceptedValues: List[String] =
    List(Auto, Static, Simulated, WallClock).map(_.name)

  implicit val read: Read[TimeProviderType] = new Read[TimeProviderType] {
    override def arity: Int = 1

    override def reads: String => TimeProviderType = { s =>
      unapply(s).getOrElse(
        throw new IllegalArgumentException(
          s"$s is not a valid time provider. The following values are accepted: ${acceptedValues
              .mkString(", ")}."
        )
      )
    }
  }

}
