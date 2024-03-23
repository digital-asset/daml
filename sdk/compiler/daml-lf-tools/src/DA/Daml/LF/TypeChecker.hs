-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker
  ( Error (..)
  , checkPackage
  , checkModule
  , nameCheckPackage
  , errorLocation
  ) where

import qualified Data.NameMap as NM

import           DA.Daml.LF.Ast
import qualified DA.Daml.LF.TypeChecker.Check      as Check
import           DA.Daml.LF.TypeChecker.Env
import           DA.Daml.LF.TypeChecker.Error
import qualified DA.Daml.LF.TypeChecker.Keyability as Keyability
import qualified DA.Daml.LF.TypeChecker.Recursion as Recursion
import qualified DA.Daml.LF.TypeChecker.Serializability as Serializability
import qualified DA.Daml.LF.TypeChecker.NameCollision as NameCollision
import Development.IDE.Types.Diagnostics

checkModule ::
     World
  -> Version
  -> Module
  -> [Diagnostic]
checkModule world0 version m = do
  checkModuleInWorld (extendWorldSelf m world0) version m

checkPackage ::
     World
  -> Version
  -> [Diagnostic]
checkPackage world version = concatMap (checkModuleInWorld world version) modules
    where
      package = getWorldSelf world
      modules = NM.toList (packageModules package)

checkModuleInWorld :: World -> Version -> Module -> [Diagnostic]
checkModuleInWorld world version m =
    case typeCheckResult of
        Left err -> toDiagnostic DsError err : collisionDiags
        Right () -> collisionDiags
  where
    collisionDiags = NameCollision.runCheckModuleDeps world m
    typeCheckResult = runGamma world version $ do
        -- We must call `Recursion.checkModule` before `Check.checkModule`
        -- or else we might loop, attempting to expand recursive type synonyms
        Recursion.checkModule m
        Check.checkModule m
        Serializability.checkModule m
        Keyability.checkModule m

-- | Check whether the whole package satisfies the name collision condition.
nameCheckPackage :: Package -> [Diagnostic]
nameCheckPackage = NameCollision.runCheckPackage
