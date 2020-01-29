// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.digitalasset.daml.lf.data.Ref.{
  ContractIdString,
  Identifier,
  LedgerString,
  Name,
  `Name equal instance`,
}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.LanguageVersion

import scala.annotation.tailrec
import scalaz.Equal
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.equal._

/** Values   */
sealed abstract class Value[+Cid] extends Product with Serializable {
  import Value._
  // TODO (FM) make this tail recursive
  def mapContractId[Cid2](f: Cid => Cid2): Value[Cid2] =
    // TODO (FM) make this tail recursive
    this match {
      case ValueContractId(coid) => ValueContractId(f(coid))
      case ValueRecord(id, fs) =>
        ValueRecord(id, fs.map({
          case (lbl, value) => (lbl, value.mapContractId(f))
        }))
      case ValueStruct(fs) =>
        ValueStruct(fs.map[(Name, Value[Cid2])] {
          case (lbl, value) => (lbl, value.mapContractId(f))
        })
      case ValueVariant(id, variant, value) =>
        ValueVariant(id, variant, value.mapContractId(f))
      case x: ValueCidlessLeaf => x
      case ValueList(vs) =>
        ValueList(vs.map(_.mapContractId(f)))
      case ValueOptional(x) => ValueOptional(x.map(_.mapContractId(f)))
      case ValueTextMap(x) => ValueTextMap(x.mapValue(_.mapContractId(f)))
      case ValueGenMap(entries) =>
        ValueGenMap(entries.map { case (k, v) => k.mapContractId(f) -> v.mapContractId(f) })
    }

  /** returns a list of validation errors: if the result is non-empty the value is
    * _not_ serializable.
    *
    * note that this does not check the validity of the [[Identifier]]s, it just checks
    * that the shape of the value is serializable.
    */
  def serializable(): ImmArray[String] = {
    @tailrec
    def go(
        exceededNesting: Boolean,
        errs: BackStack[String],
        vs0: FrontStack[(Value[Cid], Int)],
    ): BackStack[String] = vs0 match {
      case FrontStack() => errs

      case FrontStackCons((v, nesting), vs) =>
        // we cannot define helper functions because otherwise go is not tail recursive. fun!
        val exceedsNestingErr = s"exceeds maximum nesting value of $MAXIMUM_NESTING"
        val newNesting = nesting + 1

        v match {
          case tpl: ValueStruct[Cid] =>
            go(exceededNesting, errs :+ s"contains struct $tpl", vs)
          case ValueRecord(_, flds) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded the nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              go(exceededNesting, errs, flds.map(v => (v._2, newNesting)) ++: vs)
            }

          case ValueList(values) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded the nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              go(exceededNesting, errs, values.toImmArray.map(v => (v, newNesting)) ++: vs)
            }

          case ValueVariant(_, _, value) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded the nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              go(exceededNesting, errs, (value, newNesting) +: vs)
            }

          case _: ValueCidlessLeaf | _: ValueContractId[Cid] =>
            go(exceededNesting, errs, vs)
          case ValueOptional(x) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              go(exceededNesting, errs, ImmArray(x.toList.map(v => (v, newNesting))) ++: vs)
            }
          case ValueTextMap(value) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded the nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              go(exceededNesting, errs, value.values.map(v => (v, newNesting)) ++: vs)
            }
          case ValueGenMap(entries) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded the nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              val vs1 = entries.foldLeft(vs) {
                case (acc, (k, v)) => (k -> newNesting) +: (v -> newNesting) +: acc
              }
              go(exceededNesting, errs, vs1)
            }
        }
    }

    go(false, BackStack.empty, FrontStack((this, 0))).toImmArray
  }
}

object Value {

  /** the maximum nesting level for DAML-LF serializable values. we put this
    * limitation to be able to reliably implement stack safe programs with it.
    * right now it's 100 to be conservative -- it's in the same order of magnitude
    * as the default maximum nesting value of protobuf.
    *
    * encoders and decoders should check this to make sure values do not exceed
    * this level of nesting.
    */
  val MAXIMUM_NESTING: Int = 100

  final case class VersionedValue[+Cid](version: ValueVersion, value: Value[Cid]) {
    def mapContractId[Cid2](f: Cid => Cid2): VersionedValue[Cid2] =
      this.copy(value = value.mapContractId(f))

    /** Increase the `version` if appropriate for `languageVersions`. */
    def typedBy(languageVersions: LanguageVersion*): VersionedValue[Cid] = {
      import com.digitalasset.daml.lf.transaction.VersionTimeline, VersionTimeline._, Implicits._
      copy(version =
        latestWhenAllPresent(version, languageVersions map (a => a: SpecifiedVersion): _*),
      )
    }
  }

  object VersionedValue {
    implicit def `VersionedValue Equal instance`[Cid: Equal]: Equal[VersionedValue[Cid]] =
      ScalazEqual.withNatural(Equal[Cid].equalIsNatural) { (a, b) =>
        import a._
        val VersionedValue(bVersion, bValue) = b
        version == bVersion && value === bValue
      }
  }

  /** The parent of all [[Value]] cases that cannot possibly have a Cid.
    * NB: use only in pattern-matching [[Value]]; the ''type'' of a cid-less
    * Value is `Value[Nothing]`.
    */
  sealed abstract class ValueCidlessLeaf extends Value[Nothing]

  final case class ValueRecord[+Cid](
      tycon: Option[Identifier],
      fields: ImmArray[(Option[Name], Value[Cid])],
  ) extends Value[Cid]
  final case class ValueVariant[+Cid](tycon: Option[Identifier], variant: Name, value: Value[Cid])
      extends Value[Cid]
  final case class ValueEnum(tycon: Option[Identifier], value: Name) extends ValueCidlessLeaf

  final case class ValueContractId[+Cid](value: Cid) extends Value[Cid]

  /**
    * DAML-LF lists are basically linked lists. However we use FrontQueue since we store list-literals in the DAML-LF
    * packages and FrontQueue lets prepend chunks rather than only one element.
    */
  final case class ValueList[+Cid](values: FrontStack[Value[Cid]]) extends Value[Cid]
  final case class ValueInt64(value: Long) extends ValueCidlessLeaf
  final case class ValueNumeric(value: Numeric) extends ValueCidlessLeaf
  // Note that Text are assume to be UTF8
  final case class ValueText(value: String) extends ValueCidlessLeaf
  final case class ValueTimestamp(value: Time.Timestamp) extends ValueCidlessLeaf
  final case class ValueDate(value: Time.Date) extends ValueCidlessLeaf
  final case class ValueParty(value: Ref.Party) extends ValueCidlessLeaf
  final case class ValueBool(value: Boolean) extends ValueCidlessLeaf
  object ValueBool {
    val True = new ValueBool(true)
    val Fasle = new ValueBool(false)
    def apply(value: Boolean): ValueBool =
      if (value) ValueTrue else ValueFalse
  }
  case object ValueUnit extends ValueCidlessLeaf
  final case class ValueOptional[+Cid](value: Option[Value[Cid]]) extends Value[Cid]
  final case class ValueTextMap[+Cid](value: SortedLookupList[Value[Cid]]) extends Value[Cid]
  final case class ValueGenMap[+Cid](entries: ImmArray[(Value[Cid], Value[Cid])]) extends Value[Cid]
  // this is present here just because we need it in some internal code --
  // specifically the scenario interpreter converts committed values to values and
  // currently those can be structs, although we should probably ban that.
  final case class ValueStruct[+Cid](fields: ImmArray[(Name, Value[Cid])]) extends Value[Cid]

  // Order of GenMap entries is relevant for this equality.
  implicit def `Value Equal instance`[Cid: Equal]: Equal[Value[Cid]] =
    ScalazEqual.withNatural(Equal[Cid].equalIsNatural) {
      ScalazEqual.match2(fallback = false) {
        case a @ (_: ValueInt64 | _: ValueNumeric | _: ValueText | _: ValueTimestamp |
            _: ValueParty | _: ValueBool | _: ValueDate | ValueUnit) => { case b => a == b }
        case r: ValueRecord[Cid] => {
          case ValueRecord(tycon2, fields2) =>
            import r._
            tycon == tycon2 && fields === fields2
        }
        case v: ValueVariant[Cid] => {
          case ValueVariant(tycon2, variant2, value2) =>
            import v._
            tycon == tycon2 && variant == variant2 && value === value2
        }
        case v: ValueEnum => {
          case ValueEnum(tycon2, value2) =>
            import v._
            tycon == tycon2 && value == value2
        }
        case ValueContractId(value) => {
          case ValueContractId(value2) =>
            value === value2
        }
        case ValueList(values) => {
          case ValueList(values2) =>
            values === values2
        }
        case ValueOptional(value) => {
          case ValueOptional(value2) =>
            value === value2
        }
        case ValueStruct(fields) => {
          case ValueStruct(fields2) =>
            fields === fields2
        }
        case ValueTextMap(map1) => {
          case ValueTextMap(map2) =>
            map1 === map2
        }
        case genMap: ValueGenMap[Cid] => {
          case ValueGenMap(entries2) =>
            genMap.entries === entries2
        }
      }
    }

  /** A contract instance is a value plus the template that originated it. */
  final case class ContractInst[+Val](template: Identifier, arg: Val, agreementText: String) {
    def mapValue[Val2](f: Val => Val2): ContractInst[Val2] =
      this.copy(arg = f(arg))
  }

  object ContractInst {
    implicit def equalInstance[Val: Equal]: Equal[ContractInst[Val]] =
      ScalazEqual.withNatural(Equal[Val].equalIsNatural) { (a, b) =>
        import a._
        val ContractInst(bTemplate, bArg, bAgreementText) = b
        template == bTemplate && arg === bArg && agreementText == bAgreementText
      }
  }

  type NodeIdx = Int

  /** Possibly relative contract identifiers.
    *
    * The contract identifiers can be either absolute, referring to a
    * specific instance in the contract store, or relative, referring
    * to a contract created in the same transaction and hence not yet
    * having been assigned an absolute identifier.
    *
    * Note that relative contract ids are useful only before commit, in
    * the context of a transaction. After committing we should never
    * mention them.
    *
    * Why put it here and not just in Transaction.scala? Because we want
    * to be able to use AbsoluteContractId elsewhere, so that we can
    * automatically upcast to ContractId by subtyping.
    */
  sealed trait ContractId extends Product with Serializable
  final case class AbsoluteContractId(coid: ContractIdString) extends ContractId
  final case class RelativeContractId(txnid: NodeId, discriminator: Option[crypto.Hash] = None)
      extends ContractId

  object ContractId {
    implicit val equalInstance: Equal[ContractId] = Equal.equalA
  }

  /** The constructor is private so that we make sure that only this object constructs
    * node ids -- we don't want external code to manipulate them.
    */
  final case class NodeId(index: Int, discriminator: Option[crypto.Hash] = None) extends Equals {
    val name: LedgerString = LedgerString.assertFromString(index.toString)

    override def equals(other: Any): Boolean = other match {
      case that: NodeId if (this.discriminator == that.discriminator) =>
        this.discriminator.isDefined || this.index == that.index
      case _ =>
        false
    }

    override def hashCode(): Int =
      discriminator.fold(index.hashCode)(_.hashCode())

  }

  /*** Keys cannot contain contract ids */
  type Key = Value[Nothing]

  val ValueTrue: ValueBool = ValueBool.True
  val ValueFalse: ValueBool = ValueBool.Fasle
  val ValueNil: ValueList[Nothing] = ValueList(FrontStack.empty)
  val ValueNone: ValueOptional[Nothing] = ValueOptional(None)
}
