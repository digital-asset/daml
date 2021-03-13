package com.daml.platform.index

case class MutableContractStateCacheConfig(
    stateCacheSize: Int,
    keyCacheSize: Int,
)

object MutableContractStateCacheConfig {
  val default: MutableContractStateCacheConfig = MutableContractStateCacheConfig(
    100000,
    100000,
  )
}
