# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

STABLE_PACKAGES = {
    "GHC.Types": "lf-v{major}/daml-prim/GHC-Types.dalf",
    "GHC.Prim": "lf-v{major}/daml-prim/GHC-Prim.dalf",
    "GHC.Tuple": "lf-v{major}/daml-prim/GHC-Tuple.dalf",
    "DA.Internal.Erased": "lf-v{major}/daml-prim/DA-Internal-Erased.dalf",
    "DA.Internal.NatSyn": "lf-v{major}/daml-prim/DA-Internal-NatSyn.dalf",
    "DA.Internal.PromotedText": "lf-v{major}/daml-prim/DA-Internal-PromotedText.dalf",
    "DA.Exception.GeneralError": "lf-v{major}/daml-prim/DA-Exception-GeneralError.dalf",
    "DA.Exception.ArithmeticError": "lf-v{major}/daml-prim/DA-Exception-ArithmeticError.dalf",
    "DA.Exception.AssertionFailed": "lf-v{major}/daml-prim/DA-Exception-AssertionFailed.dalf",
    "DA.Exception.PreconditionFailed": "lf-v{major}/daml-prim/DA-Exception-PreconditionFailed.dalf",
    "DA.Types": "lf-v{major}/daml-prim/DA-Types.dalf",
    "DA.Time.Types": "lf-v{major}/daml-stdlib/DA-Time-Types.dalf",
    "DA.NonEmpty.Types": "lf-v{major}/daml-stdlib/DA-NonEmpty-Types.dalf",
    "DA.Date.Types": "lf-v{major}/daml-stdlib/DA-Date-Types.dalf",
    "DA.Semigroup.Types": "lf-v{major}/daml-stdlib/DA-Semigroup-Types.dalf",
    "DA.Set.Types": "lf-v{major}/daml-stdlib/DA-Set-Types.dalf",
    "DA.Monoid.Types": "lf-v{major}/daml-stdlib/DA-Monoid-Types.dalf",
    "DA.Logic.Types": "lf-v{major}/daml-stdlib/DA-Logic-Types.dalf",
    "DA.Validation.Types": "lf-v{major}/daml-stdlib/DA-Validation-Types.dalf",
    "DA.Internal.Down": "lf-v{major}/daml-stdlib/DA-Internal-Down.dalf",
    # These types are not serializable but they leak into typeclass methods so they need to be stable.,
    "DA.Internal.Any": "lf-v{major}/daml-stdlib/DA-Internal-Any.dalf",
    "DA.Internal.Template": "lf-v{major}/daml-stdlib/DA-Internal-Template.dalf",
    "DA.Internal.Interface.AnyView.Types": "lf-v{major}/daml-stdlib/DA-Internal-Interface-AnyView-Types.dalf",
    # These types are not serializable but they need to be stable so users can reuse functions from data-dependencies.,
    "DA.Action.State.Type": "lf-v{major}/daml-stdlib/DA-Action-State-Type.dalf",
    "DA.Random.Types": "lf-v{major}/daml-stdlib/DA-Random-Types.dalf",
    "DA.Stack.Types": "lf-v{major}/daml-stdlib/DA-Stack-Types.dalf",
}

def stable_packages(majorVersion):
    return {
        k: v.format(major = majorVersion)
        for (k, v) in STABLE_PACKAGES.items()
    }
