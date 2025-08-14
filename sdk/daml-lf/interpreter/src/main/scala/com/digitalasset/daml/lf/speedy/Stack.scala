package com.digitalasset.daml.lf.speedy

// We force S to be an AnyRef like that we do not need a ClassTag for S
private[speedy] class Stack[X <: AnyRef](initialCapacity: Int) extends Iterable[X] {

  assert(0 < initialCapacity)

  private[this] var array: Array[AnyRef] = Array.ofDim[AnyRef](initialCapacity)
  private[this] var size_ : Int = 0

  def capacity: Int = array.length

  override def size: Int = size_

  private def grow: Int = {
    val capacity = this.capacity
    val oldArray = array
    array = Array.ofDim(capacity * 2)
    Array.copy(oldArray, 0, array, 0, capacity)
    capacity
  }

  def push(value: X): Int = {
    val increase = if (size < capacity) 0 else grow
    array(size_) = value
    size_ += 1
    increase
  }

//  def pop: X = {
//    val x = array(size_ - 1)
//    size_ -= 1
//    array(size_ - 1) = null
//    x.asInstanceOf[X]
//  }

  // keep the first n elements of the stack
  def keep(n: Int): Unit = {
    if (n > size_)
      throw new IllegalArgumentException(s"Cannot keep $n elements, stack size is $size")
    var i = n
    while (i < size_) {
      array(i) = null // drop references
      i += 1
    }
    size_ = n
  }

  // Variables which reside on the stack. Indexed (from 1) by relative offset from the top of the stack (1 is top!)
  def apply(i: Int): X = {
    assert(0 < i && i <= size_)
    array(size_ - i).asInstanceOf[X]
  }

  override def iterator: Iterator[X] = array.iterator.take(size).asInstanceOf[Iterator[X]]

  override def knownSize: Int = size


}
