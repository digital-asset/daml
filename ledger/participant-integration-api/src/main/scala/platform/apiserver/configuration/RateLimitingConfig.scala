package com.daml.platform.apiserver.configuration

final case class RateLimitingConfig(
    maxApiServicesQueueSize: Int
)

case object RateLimitingConfig {
  val default: RateLimitingConfig = RateLimitingConfig(maxApiServicesQueueSize = 1000)
}
