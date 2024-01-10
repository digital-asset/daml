-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

pattern Daml2ScriptPackage :: GHC.UnitId
pattern Daml2ScriptPackage <- (T.stripPrefix "daml-script-" . fsToText . unitIdFS -> Just _)
pattern Daml2ScriptModule :: GHC.Module
pattern Daml2ScriptModule <- ModuleIn Daml2ScriptPackage "Daml.Script"

pattern Daml3ScriptPackage :: GHC.UnitId
pattern Daml3ScriptPackage <- (T.stripPrefix "daml3-script-" . fsToText . unitIdFS -> Just _)
pattern Daml3ScriptInternalModule :: GHC.Module
pattern Daml3ScriptInternalModule <- ModuleIn Daml3ScriptPackage "Daml.Script.Internal.LowLevel"

substUnit :: TyVar -> Type -> Type
substUnit tyVar ty = TyCoRep.substTy (setTvSubstEnv emptyTCvSubst $ mkVarEnv [(tyVar, TyConApp unitTyCon [])]) ty

isDamlScriptType :: TyCon -> Bool
isDamlScriptType (NameIn Daml2ScriptModule "Script") = True
isDamlScriptType (NameIn Daml3ScriptInternalModule "Script") = True
isDamlScriptType _ = False

topLevelWarnings :: (Var, Expr Var) -> ConvertM ()
topLevelWarnings (name, _x)
  -- Script definitions of the form `forall a. Script _` where we assume `a` is used in `_` should thrown `not executed` warnings
  -- Taking the simplest example of `forall a. Script a`, a value of this type can only exist if the final call to this expression is `error` or alike.
  -- This is most likely a top level definition of the form `myTest = script do ...; error "Got here"`, a common form for making assertions about where in a test execution reached
  -- However, the ScriptRunner does not support polymorphism, so we warn the user that this test will not run, unless they are explicit about `a`.
  -- We suggest either adding a full type signature, with `a` qualified to `()`, or using type application on `script` creating the form
  -- `myTest = script @() do ...; error "Got here"`
  | (ForAllTy (Bndr tyVar Inferred) ty@(TypeCon scriptType _)) <- varType name -- Only inferred forall, explicit forall is acceptable. Script : * -> *
  , isDamlScriptType scriptType
  = withRange (convNameLoc name) $ conversionWarning $ unlines
      [ "This method is implicitly polymorphic. Top level polymorphic scripts will not be run as tests."
      , "If this is intentional, please write an explicit type signature with the `forall'"
      , "If not, either provide a specialised type signature, such as:"
      , showSDocUnsafe $ quotes $ ppr name <+> ":" <+> pprType (substUnit tyVar ty)
      , "Or apply the unit type to the top level `script' call:"
      , showSDocUnsafe $ quotes $ ppr name <+> "= script @() do ..."
      ]
  | otherwise = pure ()
