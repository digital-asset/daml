// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

case class Versioned[+X](version: SerializationVersion, unversioned: X) {

  def map[Y](f: X => Y): Versioned[Y] = Versioned(version, f(unversioned))

  def setVersion(version: SerializationVersion): Versioned[X] = Versioned(version, unversioned)

}
