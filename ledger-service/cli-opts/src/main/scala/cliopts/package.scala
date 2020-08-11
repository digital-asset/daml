package com.daml

package object cliopts {

  /** A lens-style setter.  When you want nested structures to be possible, this
    * is vastly superior to the more obvious `(B, T) => T`, because unlike that
    * one, this form permits nesting via trivial composition.
    */
  type Setter[T, B] = (B => B, T) => T
}
