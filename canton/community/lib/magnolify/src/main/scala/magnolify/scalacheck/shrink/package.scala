// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package magnolify.scalacheck

import magnolia1.{CaseClass, Magnolia, SealedTrait}
import org.scalacheck.Shrink

import scala.annotation.nowarn
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

package object shrink {
  object semiauto {

    /** Semi-automatic derivation of [[DerivedShrink]] instances for case classes and sealed traits thereof.
      */
    @nowarn("cat=deprecation")
    object DerivedShrinkDerivation {
      type Typeclass[T] = DerivedShrink[T]

      def join[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] = DerivedShrink.from { x =>
        // Shrink each parameter individually rather than looking at all combinations
        // similar to how `Shrink.shrinkTuple*` works. This makes sense because if a shrunk value
        // is a witness to the property violation, the shrinking algorithm will try to shrink this value again.
        caseClass.parameters.toStream.flatMap { param =>
          param.typeclass.shrink
            .shrink(param.dereference(x))
            .map { shrunkParamVal =>
              caseClass.construct { p =>
                if (p == param) shrunkParamVal else p.dereference(x)
              }
            }
        }
      }

      def split[T](sealedTrait: SealedTrait[Typeclass, T]): Typeclass[T] = DerivedShrink.from { x =>
        sealedTrait.split(x)(subtype => subtype.typeclass.shrink.shrink(subtype.cast(x)))
      }

      implicit def apply[T]: Typeclass[T] = macro Magnolia.gen[T]
    }

    /** Semi-automatic derivation of [[org.scalacheck.Shrink]] instances for case classes and sealed traits thereof.
      * Derivation goes via [[magnolify.scalacheck.shrink.DerivedShrink]] so that derivation does not fall
      * back to the unshrinkable [[org.scalacheck.Shrink.shrinkAny]] default. This means that implicits
      * for [[DerivedShrink]] must be in scope for all non-derived data types, even if the derivation is for
      * [[org.scalacheck.Shrink]].
      */
    object ShrinkDerivation {
      def genShrinkMacro[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
        import c.universe.*
        val wtt = weakTypeTag[T]
        q"""_root_.magnolify.scalacheck.shrink.semiauto.DerivedShrinkDerivation.apply[$wtt].shrink"""
      }

      def apply[T]: Shrink[T] = macro genShrinkMacro[T]
    }
  }

  /** Automatic derivation of [[DerivedShrink]] and [[org.scalacheck.Shrink]] instances for case classes and
    * sealed traits thereof.
    */
  object auto {

    implicit def genShrink[T]: Shrink[T] = macro semiauto.ShrinkDerivation.genShrinkMacro[T]

    def genDeriveShrinkMacro[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
      import c.universe.*
      val wtt = weakTypeTag[T]
      q"""_root_.magnolify.scalacheck.shrink.semiauto.DerivedShrinkDerivation.apply[$wtt]"""
    }

    /** This implicit must be in scope for fully automatic derivation so that the compiler picks it up
      * when asked to derive instances for argument types.
      */
    implicit def genDerivedShrink[T]: DerivedShrink[T] = macro genDeriveShrinkMacro[T]
  }
}
