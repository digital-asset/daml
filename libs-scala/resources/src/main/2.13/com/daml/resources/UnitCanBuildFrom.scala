package com.daml.resources

import scala.collection.compat._
import scala.collection.mutable

final class UnitCanBuildFrom[T, C[_]] extends Factory[T, Unit] {
  override def fromSpecific(it: IterableOnce[T]) = ()
  override def newBuilder: mutable.Builder[T, Unit] = UnitBuilder
}

object UnitBuilder extends mutable.Builder[Any, Unit] {
  override def addOne(elem: Any): this.type = this

  override def clear(): Unit = ()

  override def result(): Unit = ()
}
