// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.crypto

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util

import com.digitalasset.daml.lf.data.Utf8
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

class Hash private (private val bytes: Array[Byte]) {

  def toByteArray: Array[Byte] = bytes.clone()

  override def toString: String = bytes.map("%02x" format _).mkString

  override def equals(other: Any): Boolean =
    other match {
      case otherHash: Hash => util.Arrays.equals(bytes, otherHash.bytes)
      case _ => false
    }

  private var _hashCode: Int = 0

  override def hashCode(): Int = {
    if (_hashCode == 0) {
      val code = util.Arrays.hashCode(bytes)
      _hashCode = if (code == 0) 1 else code
    }
    _hashCode
  }

}

object Hash {

  val DummyHash: Hash = new Hash(Array.fill(32)(0))

  sealed abstract class HashBuilder private[Hash] {

    def add(a: Array[Byte]): HashBuilder

    def add(a: Hash): HashBuilder = {
      if (a.bytes.isEmpty)
        throw new IllegalArgumentException("empty Hash")
      add(a.bytes)
    }

    def add(s: String): HashBuilder = {
      val a = Utf8.getBytes(s)
      add(a.length).add(a)
    }

    private val intBuffer = ByteBuffer.allocate(java.lang.Integer.BYTES)

    def add(a: Int): HashBuilder = {
      intBuffer.rewind()
      add(intBuffer.putInt(a).array())
    }

    private val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    def add(a: Long): HashBuilder = {
      longBuffer.rewind()
      add(longBuffer.putLong(a).array())
    }

    def iterateOver[T](i: Iterator[T], length: Int)(
        f: (HashBuilder, T) => HashBuilder): HashBuilder =
      i.foldLeft(add(length))(f)

    def build: Hash
  }

  def hashBuilder(purpose: HashPurpose): HashBuilder = new HashBuilder {

    private val md = MessageDigest.getInstance("SHA-256")

    override def add(a: Array[Byte]): HashBuilder = {
      md.update(a)
      this
    }

    add(purpose.id)

    override def build: Hash = new Hash(md.digest)
  }

  private val hMacAlgorithm = "HmacSHA256"

  def hMacBuilder(key: Hash): HashBuilder = new HashBuilder {

    private val mac: Mac = Mac.getInstance(hMacAlgorithm)

    mac.init(new SecretKeySpec(key.bytes, hMacAlgorithm))

    override def add(a: Array[Byte]): HashBuilder = {
      mac.update(a)
      this
    }

    override def build: Hash = new Hash(mac.doFinal())
  }

  def fromString(s: String): Hash = {
    import Character.digit

    val len = s.length
    if (len % 2 != 0)
      throw new IllegalArgumentException("string should contain an even number of characters")
    val a = Array.ofDim[Byte](len / 2)
    var i = 0
    while (i < len) {
      a(i / 2) = ((digit(s.charAt(i), 16) << 4) + digit(s.charAt(i + 1), 16)).toByte
      i += 2
    }
    new Hash(a)
  }

}
