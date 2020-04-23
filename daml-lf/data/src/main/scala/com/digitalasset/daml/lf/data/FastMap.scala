package com.daml.lf.data

final class FastMap[Key, Value](map: Iterable[(Key, Value)]) extends Iterable[(Key, Value)] {

  private val capacity = map.size * 4 / 3 + 1
  private val entries = Array.fill(capacity)(List.empty[(Key, Value)])

  map.foreach {
    case entry @ (key, _) =>
      val i = {
        val i = key.hashCode() % capacity
        if (i < 0) i + capacity else i
      }
      entries(i) = entry :: entries(i)
  }

  def get(key: Key): Option[Value] = {
    val i = key.hashCode() % capacity
    entries(if (i < 0) i + capacity else i).collectFirst {
      case (`key`, value) => value
    }
  }

  override def iterator: Iterator[(Key, Value)] =
    entries.iterator.flatten

}
