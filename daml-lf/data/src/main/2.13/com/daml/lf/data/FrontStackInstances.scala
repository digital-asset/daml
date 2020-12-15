// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import ScalazEqual.{equalBy, toIterableForScalazInstances}

import scalaz.Equal

abstract class FrontStackInstances {
  implicit def equalInstance[A: Equal]: Equal[FrontStack[A]] = {
    import scalaz.std.iterable._
    equalBy(fs => toIterableForScalazInstances(fs.iterator), true)
  }
}
