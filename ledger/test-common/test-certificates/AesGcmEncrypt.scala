// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package encrypt

import org.apache.commons.codec.binary.Hex

import java.io.File
import java.nio.file.Files
import javax.crypto.Cipher
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}

object AesGcmEncrypt extends App {

  val inputFile = args(0)
  val outputFile = args(1)
  val keyInHex = args(2)
  val ivInHex = args(3)
  val tagLength = args(4).toInt

  val key: Array[Byte] = Hex.decodeHex(keyInHex)
  val secretKey = new SecretKeySpec(key, "AES")
  val iv: Array[Byte] = Hex.decodeHex(ivInHex)
  val cipher = Cipher.getInstance("AES/GCM/NoPadding")
  val ivParameterSpec = new GCMParameterSpec(tagLength, iv)
  cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivParameterSpec)

  val binary = Files.readAllBytes(new File(inputFile).toPath)
  val encrypted = cipher.doFinal(binary)
  Files.write(new File(outputFile).toPath, encrypted)

  println(s"Wrote encrypted content of file $inputFile to file $outputFile")
}
