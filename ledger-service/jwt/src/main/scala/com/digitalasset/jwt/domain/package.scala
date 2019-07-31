package com.digitalasset.jwt

import java.io.File

package object domain {
  final case class Keys(publicKey: File, privateKey: File)
  final case class Jwt(value: String)
}
