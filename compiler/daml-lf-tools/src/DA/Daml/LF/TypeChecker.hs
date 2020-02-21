-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker
  ( Error (..)
  , checkPackage
  , checkModule
  , nameCheckPackage
  , errorLocation
  ) where

import           Control.Monad (forM_)
import qualified Data.NameMap as NM

import           DA.Daml.LF.Ast
import qualified DA.Daml.LF.TypeChecker.Check      as Check
import           DA.Daml.LF.TypeChecker.Env
import           DA.Daml.LF.TypeChecker.Error
import qualified DA.Daml.LF.TypeChecker.PartyLiterals as PartyLits
import qualified DA.Daml.LF.TypeChecker.Recursion as Recursion
import qualified DA.Daml.LF.TypeChecker.Serializability as Serializability
import qualified DA.Daml.LF.TypeChecker.NameCollision as NameCollision

checkModule ::
     World
  -> Version
  -> Module
  -> Either Error ()
checkModule world0 version m = do
  checkModuleInWorld (extendWorldSelf m world0) version m

checkPackage ::
     World
  -> Version
  -> Either Error ()
checkPackage world version = do
  let package = getWorldSelf world
  let modules = NM.toList (packageModules package)
  forM_ modules $ \m -> do
    checkModuleInWorld world version m

checkModuleInWorld :: World -> Version -> Module -> Either Error ()
checkModuleInWorld world version m = do
    runGamma world version $ do
      -- We must call `Recursion.checkModule` before `Check.checkModule`
      -- or else we might loop, attempting to expand recursive type synonyms
      Recursion.checkModule m
      Check.checkModule m
      Serializability.checkModule m
      PartyLits.checkModule m
    NameCollision.runCheckModuleDeps world m

-- | Check whether the whole package satisfies the name collision condition.
nameCheckPackage :: Package -> Either Error ()
nameCheckPackage = NameCollision.runCheckPackage
