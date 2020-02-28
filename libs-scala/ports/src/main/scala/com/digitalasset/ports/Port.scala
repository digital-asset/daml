// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ports

import scala.util.{Failure, Success, Try}

case class Port private (value: Int) {
  override def toString: String = value.toString
}

object Port {
  private val ValidPorts: Range = 1 until 0x10000

  /**
    * This instructs the server to automatically choose a free port.
    */
  val Dynamic = new Port(0)

  def apply(value: Int): Port =
    create(value).get

  def validate(value: Int): Try[Unit] =
    create(value).map(_ => ())

  private def create(value: Int): Try[Port] =
    if (ValidPorts.contains(value))
      Success(new Port(value))
    else
      Failure(
        new IllegalArgumentException(
          s"Ports must be in the range ${ValidPorts.start}—${ValidPorts.last}."))
}
