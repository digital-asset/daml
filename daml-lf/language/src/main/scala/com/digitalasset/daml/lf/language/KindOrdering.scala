package com.daml.lf.language

import Ast._

object KindOrdering extends Ordering[Ast.Kind] {
  @inline
  def compare(x: Kind, y: Kind): Int = {
    var diff = 0
    var stackX = List(Iterator.single(x))
    var stackY = List(Iterator.single(y))

    @inline
    def push(xs: Iterator[Kind], ys: Iterator[Kind]): Unit = {
      stackX = xs :: stackX
      stackY = ys :: stackY
    }

    @inline
    def pop(): Unit = {
      stackX = stackX.tail
      stackY = stackY.tail
    }

    @inline
    def step(tuple: (Kind, Kind)): Unit =
      tuple match {
        case (KStar, KStar) => diff = 0
        case (KNat, KNat) => diff = 0
        case (KArrow(x1, x2), KArrow(y1, y2)) =>
          push(Iterator(x1, x2), Iterator(y1, y2))
        case (k1, k2) =>
          diff = kindRank(k1) compareTo kindRank(k2)
      }

    while (diff == 0 && stackX.nonEmpty) {
      diff = stackX.head.hasNext compare stackY.head.hasNext
      if (diff == 0)
        if (stackX.head.hasNext)
          step((stackX.head.next(), stackY.head.next()))
        else
          pop()
    }

    diff
  }

  private[this] def kindRank(kind: Ast.Kind): Int = kind match {
    case KStar => 0
    case KNat => 1
    case KArrow(_, _) => 2
  }
}
