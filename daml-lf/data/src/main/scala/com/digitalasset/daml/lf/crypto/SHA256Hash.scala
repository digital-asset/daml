// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.crypto

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util

import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, Utf8}

class SHA256Hash private (private val bytes: Array[Byte]) {

  def toByteArray: Array[Byte] = bytes.clone()

  def toHexa: Ref.LedgerString =
    Ref.LedgerString.assertFromString(bytes.map("%02x" format _).mkString)

  override def toString: String = s"Hash($toHexa)"

  override def equals(other: Any): Boolean =
    other match {
      case otherHash: SHA256Hash => util.Arrays.equals(bytes, otherHash.bytes)
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

object SHA256Hash {

  /**
    * The methods of [[Builder]] change its internal state and return `this` for convenience.
    */
  // Each builder must be build with a different parameter type
  sealed abstract class Builder[X] {

    // Do not use `This` in the body of this class.
    // See bug report https://github.com/scala/bug/issues/11849
    type This = Builder[X]

    def add(a: Array[Byte]): this.type

    def add(a: ByteBuffer): this.type

    def add(a: Byte): this.type

    final def add(a: SHA256Hash): this.type =
      add(a.bytes)

    final def add(s: String): this.type = {
      val a = Utf8.getBytes(s)
      add(a.length).add(a)
    }

    private val intBuffer = ByteBuffer.allocate(java.lang.Integer.BYTES)

    def add(a: Int): this.type = {
      intBuffer.rewind()
      add(intBuffer.putInt(a).array())
    }

    private val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    def add(a: Long): this.type = {
      longBuffer.rewind()
      add(longBuffer.putLong(a).array())
    }

    // Since `X` should be uniq for any instance of the builder,
    // we are sure that `f` deals with the same builder
    def iterateOver[T](a: ImmArray[T])(f: (Builder[X], T) => Builder[X]): Builder[X] =
      a.foldLeft(add(a.length))(f)

    def build: SHA256Hash
  }

  def builder(purpose: HashPurpose): Builder[_] = new Builder[_] {

    private val md = MessageDigest.getInstance("SHA-256")

    override def add(a: Array[Byte]): this.type = {
      md.update(a)
      this
    }

    override def add(a: ByteBuffer): this.type = {
      md.update(a)
      this
    }

    override def add(a: Byte): this.type = {
      md.update(a)
      this
    }

    add(purpose.id)

    override def build: SHA256Hash = new SHA256Hash(md.digest)

  }

}
