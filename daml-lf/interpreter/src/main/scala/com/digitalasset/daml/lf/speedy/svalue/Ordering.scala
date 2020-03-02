package com.digitalasset.daml.lf.speedy.svalue

import com.digitalasset.daml.lf.data.{FrontStack, FrontStackCons, Ref, Utf8}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object Ordering extends scala.math.Ordering[SValue] {

  private def zipAndPush[X, Y](
      xs: Seq[X],
      ys: Seq[Y],
      stack: FrontStack[(X, Y)],
  ): FrontStack[(X, Y)] =
    (xs.reverseIterator zip ys.reverseIterator).foldLeft(stack)(_.+:(_))

  private def compareIdentifier(name1: Ref.TypeConName, name2: Ref.TypeConName): Int = {
    val c1 = name1.packageId compareTo name2.packageId
    lazy val c2 = name1.qualifiedName.module compareTo name2.qualifiedName.module
    def c3 = name1.qualifiedName.name compareTo name2.qualifiedName.name
    if (c1 != 0) c1 else if (c2 != 0) c2 else c3
  }

  val builtinTypeIdx =
    List(
      Ast.BTUnit,
      Ast.BTBool,
      Ast.BTInt64,
      Ast.BTText,
      Ast.BTNumeric,
      Ast.BTTimestamp,
      Ast.BTDate,
      Ast.BTParty,
      Ast.BTContractId,
      Ast.BTArrow,
      Ast.BTOptional,
      Ast.BTList,
      Ast.BTTextMap,
      Ast.BTGenMap,
      Ast.BTAny,
      Ast.BTTypeRep,
      Ast.BTUpdate,
      Ast.BTScenario
    ).zipWithIndex.toMap

  @tailrec
  // Any two ground types (types without variable nor quanitifiers) can be compared.
  private[this] def compareType(x: Int, stack0: => FrontStack[(Ast.Type, Ast.Type)]): Int =
    stack0 match {
      case FrontStack() =>
        x
      case FrontStackCons(tuple, stack) =>
        if (x != 0) x
        else
          tuple match {
            case (Ast.TBuiltin(b1), Ast.TBuiltin(b2)) =>
              compareType(builtinTypeIdx(b1) compareTo builtinTypeIdx(b2), stack)
            case (Ast.TBuiltin(_), _) =>
              -1
            case (_, Ast.TBuiltin(_)) =>
              +1
            case (Ast.TTyCon(con1), Ast.TTyCon(con2)) =>
              compareType(compareIdentifier(con1, con2), stack)
            case (Ast.TTyCon(_), _) =>
              -1
            case (_, Ast.TTyCon(_)) =>
              +1
            case (Ast.TNat(n1), Ast.TNat(n2)) =>
              compareType(n1 compareTo n2, stack)
            case (Ast.TNat(_), _) =>
              -1
            case (_, Ast.TNat(_)) =>
              +1
            case (Ast.TStruct(fields1), Ast.TStruct(fields2)) =>
              compareType(
                math.Ordering
                  .Iterable[String]
                  .compare(fields1.toSeq.map(_._1), fields2.toSeq.map(_._1)),
                zipAndPush(fields1.toSeq.map(_._2), fields2.toSeq.map(_._2), stack)
              )
            case (Ast.TStruct(_), _) =>
              -1
            case (_, Ast.TStruct(_)) =>
              +1
            case (Ast.TApp(t11, t12), Ast.TApp(t21, t22)) =>
              compareType(0, (t11, t21) +: (t12, t22) +: stack)
            case (Ast.TApp(_, _), _) =>
              -1
            case (_, Ast.TApp(_, _)) =>
              +1
            case (t1, t2) =>
              throw new IllegalArgumentException(
                s"cannot compare types ${t1.pretty} and ${t2.pretty}")
          }
    }

  // undefined if `typ1` or `typ2` contains `TVar`, `TForAll`, or `TSynApp`.
  private def compareType(typ1: Ast.Type, typ2: Ast.Type): Int =
    compareType(0, FrontStack((typ1, typ2)))

  private def compareText(text1: String, text2: String): Int =
    Utf8.Ordering.compare(text1, text2)

  private def isHexa(s: String): Boolean =
    s.forall(x => '0' < x && x < '9' || 'a' < x && x < 'f')

  @inline
  private val underlyingCidDiscriminatorLength = 65

  private def compareCid(cid1: Ref.ContractIdString, cid2: Ref.ContractIdString): Int = {
    val lim = cid1.length min cid2.length

    @tailrec
    def lp(i: Int): Int =
      if (i < lim) {
        val x = cid1(i)
        val y = cid2(i)
        if (x != y) x compareTo y else lp(i + 1)
      } else {
        if (lim == underlyingCidDiscriminatorLength && (//
          cid1.length != underlyingCidDiscriminatorLength && isHexa(cid2) || //
          cid2.length != underlyingCidDiscriminatorLength && isHexa(cid1)))
          throw new IllegalArgumentException(
            "Try to a fresh contract id with  conflicting discriminators")
        else
          cid1.length compareTo cid2.length
      }

    lp(0)
  }
  @tailrec
  // Only value of the same type can be compared.
  private[this] def compareValue(x: Int, stack0: FrontStack[(SValue, SValue)]): Int =
    stack0 match {
      case FrontStack() =>
        x
      case FrontStackCons(tuple, stack) =>
        if (x != 0) x
        else
          tuple match {
            case (SUnit, SUnit) =>
              compareValue(0, stack)
            case (SBool(b1), SBool(b2)) =>
              compareValue(b1 compareTo b2, stack)
            case (SInt64(i1), SInt64(i2)) =>
              compareValue(i1 compareTo i2, stack)
            case (STimestamp(ts1), STimestamp(ts2)) =>
              compareValue(ts1.micros compareTo ts2.micros, stack)
            case (SDate(d1), SDate(d2)) =>
              compareValue(d1.days compareTo d2.days, stack)
            case (SNumeric(n1), SNumeric(n2)) =>
              compareValue(n1 compareTo n2, stack)
            case (SText(t1), SText(t2)) =>
              compareValue(compareText(t1, t2), stack)
            case (SParty(p1), SParty(p2)) =>
              compareValue(compareText(p1, p2), stack)
            case (SContractId(AbsoluteContractId(coid1)), SContractId(AbsoluteContractId(coid2))) =>
              compareValue(compareCid(coid1, coid2), stack)
            case (STypeRep(t1), STypeRep(t2)) =>
              compareValue(compareType(t1, t2), stack)
            case (SEnum(_, con1), SEnum(_, con2)) =>
              compareValue((con1 compareTo con2), stack)
            case (SRecord(_, _, args1), SRecord(_, _, args2)) =>
              compareValue(0, zipAndPush(args1.asScala, args2.asScala, stack))
            case (SVariant(_, con1, arg1), SVariant(_, con2, arg2)) =>
              compareValue((con1 compareTo con2), (arg1, arg2) +: stack)
            case (SList(FrontStackCons(head1, tail1)), SList(FrontStackCons(head2, tail2))) =>
              compareValue(0, (head1, head2) +: (SList(tail1), SList(tail2)) +: stack)
            case (SList(l1), SList(l2)) =>
              l1.nonEmpty compareTo l2.nonEmpty
            case (SOptional(Some(v1)), SOptional(Some(v2))) =>
              compareValue(0, (v1, v2) +: stack)
            case (SOptional(o1), SOptional(o2)) =>
              compareValue(o1.nonEmpty compareTo o2.nonEmpty, stack)
            case (map1: STextMap, map2: STextMap) =>
              compareValue(0, (toList(map1), toList(map2)) +: stack)
            case (SStruct(_, args1), SStruct(_, args2)) =>
              compareValue(0, zipAndPush(args1.asScala, args2.asScala, stack))
            case (SAny(t1, v1), SAny(t2, v2)) =>
              compareValue(compareType(t1, t2), (v1, v2) +: stack)
            case (SContractId(_), SContractId(_)) =>
              throw new IllegalAccessException("Try to compare relative contract ids")
            case (SPAP(_, _, _), SPAP(_, _, _)) =>
              throw new IllegalAccessException("Try to compare functions")
            case _ =>
              throw new IllegalAccessException("Try to compare unrelated type")
          }
    }

  def compare(v1: SValue, v2: SValue): Int =
    compareValue(0, FrontStack((v1, v2)))

}
