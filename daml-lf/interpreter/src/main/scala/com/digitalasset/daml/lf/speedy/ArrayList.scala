// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.scalautil.Statement.discard

import java.util
import scala.collection.{View, mutable}
import scala.jdk.CollectionConverters._

object ArrayList extends scala.collection.IterableFactory[util.ArrayList] {

  override def from[A](source: IterableOnce[A]): util.ArrayList[A] = {
    val array = source.knownSize match {
      case size if size >= 0 => new util.ArrayList[A](size)
      case _ => empty[A]
    }
    source.iterator.foreach(array.add)
    array
  }

  override def empty[A]: util.ArrayList[A] = new util.ArrayList()

  def single[A](a: A): util.ArrayList[A] = {
    val array = new util.ArrayList[A](1)
    discard(array.add(a))
    array
  }

  def double[A](a: A, b: A): util.ArrayList[A] = {
    val array = new util.ArrayList[A](2)
    discard(array.add(a))
    discard(array.add(b))
    array
  }

  override def newBuilder[A]: mutable.Builder[A, util.ArrayList[A]] =
    new mutable.Builder[A, util.ArrayList[A]] {
      private[this] val array = empty[A]

      override def clear(): Unit = array.clear()

      override def result(): util.ArrayList[A] = array

      override def addOne(elem: A): this.type = {
        discard[Boolean](array.add(elem))
        this
      }

      override def sizeHint(size: Int): Unit =
        array.ensureCapacity(size)
    }

  def unapplySeq[A](jl: util.ArrayList[A]): Some[collection.Seq[A]] = Some(jl.asScala)

  object Implicits {
    implicit class ArrayListOps[A](val array: util.ArrayList[A]) extends AnyVal {
      def map[B](f: A => B): util.ArrayList[B] = {
        val result = new util.ArrayList[B](array.size())
        array.forEach(a => discard(result.add(f(a))))
        result
      }

      def view: collection.View[A] = new View[A] {
        override def iterator: Iterator[A] = array.iterator().asScala
      }
    }
  }
}
