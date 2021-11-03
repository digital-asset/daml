package com.daml.lf
package transaction

case class Versioned[+X](version: TransactionVersion, unversioned: X) {

  def map[Y](f: X => Y): Versioned[Y] = Versioned(version, f(unversioned))

  def setVersion(version: TransactionVersion): Versioned[X] = Versioned(version, unversioned)

}
