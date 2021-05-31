// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.benchmark

import com.daml.lf.transaction.TransactionVersion

case class Versioned[A](version: TransactionVersion, value: A) {
  def map[B](f: A => B): Versioned[B] = Versioned(version, f(value))
  def traverse[B, C](f: A => Either[B, C]): Either[B, Versioned[C]] =
    f(value).flatMap(v => Right(Versioned(version, v)))
}
