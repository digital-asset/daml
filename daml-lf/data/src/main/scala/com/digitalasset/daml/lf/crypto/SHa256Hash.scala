// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.crypto

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util

import com.digitalasset.daml.lf.data.{Ref, Utf8}

class SHa256Hash private (private val bytes: Array[Byte]) {

  def toByteArray: Array[Byte] = bytes.clone()

  def toHexa: Ref.LedgerString =
    Ref.LedgerString.assertFromString(bytes.map("%02x" format _).mkString)

  override def toString: String = s"Hash($toHexa)"

  override def equals(other: Any): Boolean =
    other match {
      case otherHash: SHa256Hash => util.Arrays.equals(bytes, otherHash.bytes)
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

object SHa256Hash {

  /**
    * The methods of [[Builder]] change its internal state and return `this` for convenience.
    */
  sealed abstract class Builder {

    def add(a: Array[Byte]): Builder

    def add(a: ByteBuffer): Builder

    def add(a: Byte): Builder

    final def add(a: SHa256Hash): Builder =
      add(a.bytes)

    final def add(s: String): Builder = {
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

    def build: SHa256Hash
  }

  def builder(purpose: HashPurpose): Builder = new Builder {

    private val md = MessageDigest.getInstance("SHA-256")

    override def add(a: Array[Byte]): Builder = {
      md.update(a)
      this
    }

    override def add(a: ByteBuffer): Builder = {
      md.update(a)
      this
    }

    override def add(a: Byte): Builder = {
      md.update(a)
      this
    }

    add(purpose.id)

    override def build: SHa256Hash = new SHa256Hash(md.digest)

  }

}
