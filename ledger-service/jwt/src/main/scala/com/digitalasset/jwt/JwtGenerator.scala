package com.digitalasset.jwt

import scala.util.Try

object JwtGenerator {
  def generate(keys: domain.Keys): Try[domain.Jwt] = Try(domain.Jwt("dummy"))
}
