// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import java.io.ByteArrayInputStream
import java.nio.file.Files
import javax.crypto.{Cipher, KeyGenerator, SecretKey}
import org.apache.commons.codec.binary.Hex
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.Base64

class DecryptionParametersTest extends AnyWordSpec with Matchers {

  "decryption parameters" should {

    // given
    val key: SecretKey = KeyGenerator.getInstance("AES").generateKey()
    val clearText = "clearText123 " * 10
    val clearTextBytes = clearText.getBytes
    val transformation = "AES/CBC/PKCS5Padding"
    val cipher = Cipher.getInstance(transformation)
    cipher.init(Cipher.ENCRYPT_MODE, key)
    val hexEncodedKey = new String(Hex.encodeHexString(key.getEncoded))
    val hexEncodedIv = new String(Hex.encodeHex(cipher.getIV))
    val cipherText: Array[Byte] = cipher.doFinal(clearTextBytes)

    val tested = DecryptionParameters(
      transformation = transformation,
      keyInHex = hexEncodedKey,
      initializationVectorInHex = hexEncodedIv,
    )

    "decrypt a byte array" in {
      // when
      val actual: Array[Byte] = tested.decrypt(cipherText)

      // then
      actual shouldBe clearTextBytes
      new String(actual) shouldBe clearText
    }

    "decrypt a file" in
      testFileDecoding("-verbatim", cipherText)

    "decrypt a file in base64" in
      testFileDecoding("-base64", Base64.getEncoder.encode(cipherText))

    "decrypt a file in MIME base64" in
      testFileDecoding("-mime-base64", Base64.getMimeEncoder.encode(cipherText))

    def testFileDecoding(fileSuffix: String, content: Array[Byte]) = {
      // given
      val tmpFilePath = Files.createTempFile(s"cipher-text$fileSuffix", ".enc")
      Files.write(tmpFilePath, content)
      assume(Files.readAllBytes(tmpFilePath) sameElements content)

      // when
      val actual: Array[Byte] = tested.decrypt(tmpFilePath.toFile)

      // then
      actual shouldBe clearTextBytes
      new String(actual) shouldBe clearText
    }
  }

  it should {
    "extract algorithm name from long transformation string" in {
      // given
      val tested = DecryptionParameters(
        transformation = "algorithm1/mode2/padding3",
        keyInHex = "dummyKey",
        initializationVectorInHex = "dummyIv",
      )

      // when & then
      tested.algorithm shouldBe "algorithm1"
    }

    "extract algorithm name from short transformation string" in {
      // given
      val tested = DecryptionParameters(
        transformation = "algorithm1",
        keyInHex = "dummyKey",
        initializationVectorInHex = "dummyIv",
      )

      // when & then
      tested.algorithm shouldBe "algorithm1"
    }
  }

  it should {
    "parse JSON file specifying decryption parameters" in {
      // given
      val jsonPayload =
        """{
          |	"algorithm": "algorithm1/mode2/padding3",
          |	"key": "<hex encoded bytes of key>",
          |	"iv": "<hex encoded bytes of iv>",
          |	"key_length" : 123
          |}
          |""".stripMargin
      val expected = DecryptionParameters(
        transformation = "algorithm1/mode2/padding3",
        keyInHex = "<hex encoded bytes of key>",
        initializationVectorInHex = "<hex encoded bytes of iv>",
      )
      // when
      val actual = DecryptionParameters.parsePayload(jsonPayload)

      // then
      actual shouldBe expected
    }

    "fetch JSON document from a secrets URL" in {
      // given
      val expected = "decryption-params123"
      val secretsUrl: SecretsUrl = () => new ByteArrayInputStream(expected.getBytes)

      // when
      val actual = DecryptionParameters.fetchPayload(secretsUrl)

      // then
      actual shouldBe expected
    }
  }

}
