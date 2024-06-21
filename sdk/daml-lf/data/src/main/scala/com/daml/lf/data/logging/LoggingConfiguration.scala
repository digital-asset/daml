// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data.logging

case class LoggingConfiguration(
    maxPartyNameLength: Option[Int]
)

object LoggingConfiguration {
  private val Default = LoggingConfiguration(
    maxPartyNameLength = None
  )

  @volatile
  private var _current = Default

  def current: LoggingConfiguration = _current

  def modify(f: LoggingConfiguration => LoggingConfiguration): Unit = {
    _current = f(_current)
  }

  def set(configuration: LoggingConfiguration): Unit = {
    _current = configuration
  }
}
