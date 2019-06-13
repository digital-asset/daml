package com.digitalasset.ledger.client.binding

abstract class EnumRef extends ValueRef {
  val constructor: String
  val index: Int
}
