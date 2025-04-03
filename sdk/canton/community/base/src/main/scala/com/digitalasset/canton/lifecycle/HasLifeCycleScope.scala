// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

/** Parent trait for anything that needs to deal with [[LifeCycleScope]]s.
  */
sealed trait HasLifeCycleScope extends Any {
  import HasLifeCycleScope.*

  /** The type for [[LifeCycleScope]]s that have accumulated all [[LifeCycleManager]]s up to the
    * caller of the most recent call into this [[HasLifeCycleScope]] instance. It may not contain
    * the manager of this instance.
    *
    * Public methods should take this kind of [[LifeCycleScope]].
    *
    * @see
    *   OwnLifeCycleScope
    */
  final type ContextLifeCycleScope = LifeCycleScope[ContextLifeCycleScopeDiscriminator]

  /** The type for [[LifeCycleScope]]s that have accumulated all [[LifeCycleManager]]s up to and
    * including this [[HasLifeCycleScope]] instance.
    *
    * Private and protected methods should take this kind of [[LifeCycleScope]] if they are meant to
    * be called only from the same instance.
    *
    * `protected` because it rarely makes sense to use this type outside of this instance; other
    * call sites have a hard time to ensure that this instance's [[LifeCycleManager]] has already
    * been accumulated.
    *
    * Instances can only be produced by [[augmentOwnLifeCycleScope]].
    *
    * @see
    *   ContextLifeCycleScope
    */
  protected final type OwnLifeCycleScope = LifeCycleScope[OwnLifeCycleScopeDiscriminator]

  /** The discriminator that binds a context's [[LifeCycleScope]]s to an individual instance of
    * [[HasLifeCycleScope]]. It is deliberately left unspecified so that scalac cannot derive type
    * equalities for the discriminator across different instances of [[HasLifeCycleScope]].
    *
    * The abstract upper bounds here and on [[OwnLifeCycleScopeDiscriminator]] ensure that we can
    * distinguish [[ContextLifeCycleScope]]s from [[OwnLifeCycleScope]]s in implicit conversions
    * such as [[augmentContextLifeCycleScope]].
    *
    * The upper and lower bounds together ensure that implementations cannot refine this type and
    * [[OwnLifeCycleScopeDiscriminator]] so that scalac could derive a type equality between them.
    * For without either of the bounds, [[scala.Any]] or [[scala.Nothing]] would be valid
    * refinements.
    */
  type ContextLifeCycleScopeDiscriminator >: ContextLifeCycleScopeDiscriminatorLower
    <: ContextLifeCycleScopeDiscriminatorUpper

  /** The discriminator for an individual instance's own [[LifeCycleScope]]. It is deliberately left
    * unspecified so that scalac cannot derive type equalities for the discriminator across
    * different instances of [[HasLifeCycleScope]].
    *
    * The abstract upper bounds here and on [[ContextLifeCycleScopeDiscriminator]] ensure that we
    * can distinguish [[ContextLifeCycleScope]]s from [[OwnLifeCycleScope]]s in implicit conversions
    * such as [[augmentOwnLifeCycleScope]].
    *
    * The upper and lower bounds together ensure that implementations cannot refine this type and
    * [[OwnLifeCycleScopeDiscriminator]] so that scalac could derive a type equality between them.
    * For without either of the bounds, [[scala.Any]] or [[scala.Nothing]] would be valid
    * refinements.
    */
  protected type OwnLifeCycleScopeDiscriminator >: OwnLifeCycleScopeDiscriminatorLower
    <: OwnLifeCycleScopeDiscriminatorUpper

  /** Converts a [[ContextLifeCycleScope]] from this instance into a [[ContextLifeCycleScope]] of
    * another instance of [[HasLifeCycleScope]], possibly adding this [[HasLifeCycleScope]]'s
    * manager to the scope.
    *
    * @tparam A
    *   The [[HasLifeCycleScope.ContextLifeCycleScopeDiscriminator]] of the other instance. We
    *   cannot directly constrain to `A <: HasLifeCycleScope` because this would break implicit
    *   resolution in scalac. Instead, we use the bounds to ensure that the returned
    *   [[LifeCycleScope]] can only be used as a [[ContextLifeCycleScope]]. In particular, we must
    *   forbid that this method returns a scope of type [[HasLifeCycleScope.OwnLifeCycleScope]] for
    *   another instance of [[HasLifeCycleScope]].
    */
  protected[this] implicit def augmentContextLifeCycleScope[
      A >: ContextLifeCycleScopeDiscriminatorLower <: ContextLifeCycleScopeDiscriminatorUpper
  ](implicit scope: ContextLifeCycleScope): LifeCycleScope[A]

  /** Converts this instance's [[ContextLifeCycleScope]] into its [[OwnLifeCycleScope]], adding this
    * instance's [[LifeCycleManager]] to the scope if necessary.
    */
  protected[this] implicit def augmentOwnLifeCycleScope(implicit
      scope: ContextLifeCycleScope
  ): OwnLifeCycleScope

  /** Coerces the [[OwnLifeCycleScope]] into a [[ContextLifeCycleScope]] of any other instance.
    *
    * @tparam A
    *   The [[HasLifeCycleScope.ContextLifeCycleScopeDiscriminator]] of the other instance. See
    *   [[augmentContextLifeCycleScope]] for a discussion of the bounds.
    */
  protected[this] implicit def coerceOwnLifeCycleScope[
      A >: ContextLifeCycleScopeDiscriminatorLower <: ContextLifeCycleScopeDiscriminatorUpper
  ](implicit scope: OwnLifeCycleScope): LifeCycleScope[A] = scope.coerce[A]

  /** Creates a fresh [[LifeCycleScope]] without any managers in it. This should be used when a new
    * fiber of processing starts.
    */
  protected final def freshLifeCycleScope: ContextLifeCycleScope = LifeCycleScopeImpl.empty

  /** [[LifeCycleScope]] does not expose the synchronization methods of `HasSynchronizeWithClosing`
    * so that one cannot accidentally call them on a [[ContextLifeCycleScope]]. This method opens up
    * the own scope so that the synchronization methods can be used on them.
    */
  @inline
  protected[this] final def ownScope(implicit scope: OwnLifeCycleScope): HasSynchronizeWithClosing =
    scope match {
      case impl: LifeCycleScopeImpl[?] => impl
    }
}

object HasLifeCycleScope {

  /** Abstract upper bound for the [[HasLifeCycleScope.ContextLifeCycleScopeDiscriminator]]. Ensures
    * that the implicit conversion methods can distinguish these discriminators from
    * [[HasLifeCycleScope.OwnLifeCycleScopeDiscriminator]]s.
    */
  type ContextLifeCycleScopeDiscriminatorUpper

  /** Abstract lower bound for [[HasLifeCycleScope.ContextLifeCycleScopeDiscriminator]]. Ensures
    * that implementations of [[HasLifeCycleScope]] cannot refine
    * [[HasLifeCycleScope.ContextLifeCycleScopeDiscriminator]] in any meaningful way.
    */
  type ContextLifeCycleScopeDiscriminatorLower <: ContextLifeCycleScopeDiscriminatorUpper

  /** Abstract upper bound for the [[HasLifeCycleScope.OwnLifeCycleScopeDiscriminator]]. Ensures
    * that the implicit conversion methods can distinguish these discriminators from
    * [[HasLifeCycleScope.ContextLifeCycleScopeDiscriminator]]s.
    */
  type OwnLifeCycleScopeDiscriminatorUpper

  /** Abstract lower bound for [[HasLifeCycleScope.OwnLifeCycleScopeDiscriminator]]. Ensures that
    * implementations of [[HasLifeCycleScope]] cannot refine
    * [[HasLifeCycleScope.OwnLifeCycleScopeDiscriminator]] in any meaningful way.
    */
  type OwnLifeCycleScopeDiscriminatorLower <: OwnLifeCycleScopeDiscriminatorUpper
}

/** Trait for all objects that are managed by a [[LifeCycleManager]].
  * [[HasLifeCycleScope.OwnLifeCycleScope]]s always include this manager.
  */
trait ManagedLifeCycle extends HasLifeCycleScope { self =>
  import HasLifeCycleScope.*

  protected def manager: LifeCycleManager

  protected[this] final override implicit def augmentContextLifeCycleScope[
      A >: ContextLifeCycleScopeDiscriminatorLower <: ContextLifeCycleScopeDiscriminatorUpper
  ](implicit scope: ContextLifeCycleScope): LifeCycleScope[A] =
    augmentLifeCycleScope[A]

  protected[this] final override implicit def augmentOwnLifeCycleScope(implicit
      scope: ContextLifeCycleScope
  ): OwnLifeCycleScope = augmentLifeCycleScope[OwnLifeCycleScopeDiscriminator]

  private[this] def augmentLifeCycleScope[A](implicit
      scope: ContextLifeCycleScope
  ): LifeCycleScope[A] = {
    val oldManagers = scope.managers
    val withNewManager = oldManagers.incl(this.manager)
    // Sets return the same instance if the element is already in the set
    // So we can use the fast `eq` comparison to check whether something changed.
    if (withNewManager eq oldManagers) scope.coerce[A]
    else new LifeCycleScopeImpl(withNewManager)
  }
}

/** Trait for all objects that are not managed by a [[LifeCycleManager]], but that still deal with
  * [[LifeCycleScope]]s. [[OwnLifeCycleScope]] and [[ContextLifeCycleScope]]s are the same w.r.t.
  * [[LifeCycleManager]]s.
  */
trait UnmanagedLifeCycle extends Any with HasLifeCycleScope {
  import HasLifeCycleScope.*

  protected[this] final override implicit def augmentContextLifeCycleScope[
      A >: ContextLifeCycleScopeDiscriminatorLower <: ContextLifeCycleScopeDiscriminatorUpper
  ](implicit scope: ContextLifeCycleScope): LifeCycleScope[A] = scope.coerce[A]

  protected[this] final override implicit def augmentOwnLifeCycleScope(implicit
      scope: ContextLifeCycleScope
  ): OwnLifeCycleScope = scope.coerce[OwnLifeCycleScopeDiscriminator]
}
