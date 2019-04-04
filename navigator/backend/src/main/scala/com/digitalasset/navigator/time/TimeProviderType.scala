// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.time

import scopt.Read

sealed abstract class TimeProviderType extends Product with Serializable
object TimeProviderType {

  case object Auto extends TimeProviderType
  case object Static extends TimeProviderType
  case object Simulated extends TimeProviderType
  case object WallClock extends TimeProviderType

  def apply(string: String): Option[TimeProviderType] = string match {
    case auto if auto.toLowerCase() == "auto" => Some(Auto)
    case static if static.toLowerCase() == "static" => Some(Static)
    case simulated if simulated.toLowerCase() == "simulated" => Some(Simulated)
    case wallclock if wallclock.toLowerCase() == "wallclock" => Some(WallClock)
    case _ => None
  }

  def write(tpt: TimeProviderType): String = tpt match {
    case Auto => "auto"
    case Static => "static"
    case Simulated => "simulated"
    case WallClock => "wallclock"
  }

  def acceptedValues: List[String] =
    List(Auto, Static, Simulated, WallClock).map(_.toString.toLowerCase)

  implicit val read: Read[TimeProviderType] = new Read[TimeProviderType] {
    override def arity: Int = 1

    override def reads: String => TimeProviderType = { s =>
      apply(s).getOrElse(
        throw new IllegalArgumentException(
          s"$s is not a valid time provider. The following values are accepted: ${acceptedValues
            .mkString(", ")}."))
    }
  }

}
