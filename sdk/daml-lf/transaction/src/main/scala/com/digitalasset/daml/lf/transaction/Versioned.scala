// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

case class Versioned[+X](version: TransactionVersion, unversioned: X) {

  def map[Y](f: X => Y): Versioned[Y] = Versioned(version, f(unversioned))

  def setVersion(version: TransactionVersion): Versioned[X] = Versioned(version, unversioned)

}
