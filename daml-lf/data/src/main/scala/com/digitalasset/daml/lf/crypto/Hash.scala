// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.crypto

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util

import com.digitalasset.daml.lf.data.Utf8

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

  sealed abstract class Builder private[Hash] {

    def add(a: Array[Byte]): Builder

    def add(a: Hash): Builder = {
      if (a.bytes.isEmpty)
        throw new IllegalArgumentException("empty Hash")
      add(a.bytes)
    }

    def add(s: String): Builder = {
      val a = Utf8.getBytes(s)
      add(a.length).add(a)
    }

    private val intBuffer = ByteBuffer.allocate(java.lang.Integer.BYTES)

    def add(a: Int): Builder = {
      intBuffer.rewind()
      add(intBuffer.putInt(a).array())
    }

    private val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    def add(a: Long): Builder = {
      longBuffer.rewind()
      add(longBuffer.putLong(a).array())
    }

    def iterateOver[T](i: Iterator[T], length: Int)(f: (Builder, T) => Builder): Builder =
      i.foldLeft(add(length))(f)

    def build: Hash
  }

  def hashBuilder(purpose: HashPurpose): Builder = new Builder {

    private val md = MessageDigest.getInstance("SHA-256")

    override def add(a: Array[Byte]): Builder = {
      md.update(a)
      this
    }

    add(purpose.id)

    override def build: Hash = new Hash(md.digest)
  }

}
