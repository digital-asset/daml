// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

case class Versioned[+X](version: TransactionVersion, unversioned: X) {

  def map[Y](f: X => Y): Versioned[Y] = Versioned(version, f(unversioned))

  def traverse[Y, Z](f: X => Either[Y, Z]): Either[Y, Versioned[Z]] =
    f(unversioned).map(Versioned(version, _))

  @deprecated("used copy", since = "1.2.0")
  def setVersion(version: TransactionVersion): Versioned[X] = Versioned(version, unversioned)

}
