// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

final case class Configuration(maximumWeight: Long) extends AnyVal

object Configuration {

  val none: Configuration = Configuration(maximumWeight = 0)

}
