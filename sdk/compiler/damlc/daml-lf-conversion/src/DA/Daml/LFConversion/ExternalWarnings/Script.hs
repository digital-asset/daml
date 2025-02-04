-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PatternSynonyms #-}

-- | For compiler level warnings on Daml.Script
module DA.Daml.LFConversion.ExternalWarnings.Script (topLevelWarnings) where

import qualified Data.Text as T
import DA.Daml.UtilGHC
import DA.Daml.LFConversion.ConvertM
import DA.Daml.LFConversion.Utils
import "ghc-lib" GhcPlugins as GHC hiding ((<>))
import "ghc-lib" TyCoRep

pattern DamlScriptPackage :: GHC.UnitId
pattern DamlScriptPackage <- (T.stripPrefix "daml-script-" . fsToText . unitIdFS -> Just _)
pattern DamlScriptInternalModule :: GHC.Module
pattern DamlScriptInternalModule <- ModuleIn DamlScriptPackage "Daml.Script.Internal.LowLevel"

isDamlScriptType :: TyCon -> Bool
isDamlScriptType (NameIn DamlScriptInternalModule "Script") = True
isDamlScriptType _ = False

topLevelWarnings :: (Var, Expr Var) -> ConvertM ()
topLevelWarnings (name, _x)
  -- Script definitions of the form `forall a. Script _` where we assume `a` is used in `_` should thrown `not executed` warnings
  -- Taking the simplest example of `forall a. Script a`, a value of this type can only exist if the final call to this expression is `error` or alike.
  -- This is most likely a top level definition of the form `myTest = script do ...; error "Got here"`, a common form for making assertions about where in a test execution reached
  -- However, the IdeLedgerRunner does not support polymorphism, so we warn the user that this test will not run, unless they are explicit about `a`.
  -- We suggest either adding a full type signature, with `a` qualified to `()`, or using type application on `script` creating the form
  -- `myTest = script @() do ...; error "Got here"`
  | (ForAllTy (Bndr tyVar Inferred) ty@(TypeCon scriptType _)) <- varType name -- Only inferred forall, explicit forall is acceptable. Script : * -> *
  , isDamlScriptType scriptType
  = withRange (convNameLoc name) $ conversionWarning $ PolymorphicTopLevelScript name ty tyVar
  | otherwise = pure ()
