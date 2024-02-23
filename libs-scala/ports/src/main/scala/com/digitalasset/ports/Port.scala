// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import java.net.{InetAddress, Socket}

import scala.util.{Failure, Success, Try}

final case class Port private (value: Int) extends AnyVal {
  override def toString: String = value.toString

  def test(host: InetAddress): Unit = {
    val socket = new Socket(host, value)
    socket.close()
  }
}

object Port {
  private val ValidPorts: Range = 0 until 0x10000

  /** This instructs the server to automatically choose a free port.
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
          s"Ports must be in the range ${ValidPorts.start}—${ValidPorts.last}."
        )
      )
}
