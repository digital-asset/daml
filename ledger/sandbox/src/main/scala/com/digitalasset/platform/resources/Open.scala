// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import java.io.Closeable

import scala.util.control.NonFatal

trait Open[A] extends Closeable {
  def value: A

  def map[B](f: A => B): Open[B] = new Open[B] {
    override val value: B = f(Open.this.value)

    override def close(): Unit = Open.this.close()
  }

  def flatMap[B](f: A => Open[B]): Open[B] = new Open[B] {
    private val a: Open[A] = Open.this
    private val b: Open[B] =
      try {
        f(a.value)
      } catch {
        case NonFatal(e) =>
          a.close()
          throw e
      }

    override val value: B = b.value

    override def close(): Unit = {
      b.close()
      a.close()
    }
  }

  def foreach[U](f: A => U): Unit = discard(f(value))

  def withFilter(p: A => Boolean): Open[A] =
    if (p(value))
      this
    else {
      close()
      new Open[A] {
        override def value: A =
          throw new ResourceAcquisitionFilterException()

        override def close(): Unit = ()
      }
    }

  private def discard[T](value: T): Unit = ()
}
