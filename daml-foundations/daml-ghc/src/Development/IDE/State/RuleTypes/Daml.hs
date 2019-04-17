-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}

-- | This module extends the general Haskell rule types in
-- `Development.IDE.State.RuleTypes` with DAML specific rule types
-- such as those for producing DAML LF.
module Development.IDE.State.RuleTypes.Daml(
    module Development.IDE.State.RuleTypes,
    module Development.IDE.State.RuleTypes.Daml
    ) where

import Control.DeepSeq
import Data.Binary
import qualified Data.ByteString as BS
import Data.Hashable
import Data.Map.Strict (Map)
import Data.Typeable (Typeable)
import Development.Shake
import GHC.Generics (Generic)
import "ghc-lib-parser" Module (UnitId)

import Development.IDE.Types.LSP
import Development.IDE.State.RuleTypes
import Development.IDE.State.Shake

import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.ScenarioServiceClient as SS

type instance RuleResult GenerateDalf = IdeReturn LF.Module
type instance RuleResult GenerateRawDalf = IdeReturn LF.Module
type instance RuleResult GeneratePackage = IdeReturn LF.Package
type instance RuleResult GenerateRawPackage = IdeReturn LF.Package
type instance RuleResult GeneratePackageDeps = IdeReturn LF.Package
type instance RuleResult GeneratePackageMap = IdeReturn (Map UnitId (LF.PackageId, LF.Package, BS.ByteString, FilePath))

-- | Runs all scenarios in the given file (but not scenarios in imports).
type instance RuleResult RunScenarios = IdeReturn [(VirtualResource, Either SS.Error SS.ScenarioResult)]

-- | Encode a module and produce a hash of the module and all its transitive dependencies.
-- The hash is used to decide if a module needs to be reloaded in the scenario service.
type instance RuleResult EncodeModule = IdeReturn (SS.Hash, BS.ByteString)

-- | Create a scenario context for a given module. This context is valid both for the module
-- itself but also for all of its transitive dependencies.
type instance RuleResult CreateScenarioContext = IdeReturn SS.ContextId

data GenerateDalf = GenerateDalf
    deriving (Eq, Show, Typeable, Generic)
instance Binary   GenerateDalf
instance Hashable GenerateDalf
instance NFData   GenerateDalf

data GenerateRawDalf = GenerateRawDalf
    deriving (Eq, Show, Typeable, Generic)
instance Binary   GenerateRawDalf
instance Hashable GenerateRawDalf
instance NFData   GenerateRawDalf

data GeneratePackage = GeneratePackage
    deriving (Eq, Show, Typeable, Generic)
instance Binary   GeneratePackage
instance Hashable GeneratePackage
instance NFData   GeneratePackage

data GenerateRawPackage = GenerateRawPackage
    deriving (Eq, Show, Typeable, Generic)
instance Binary   GenerateRawPackage
instance Hashable GenerateRawPackage
instance NFData   GenerateRawPackage

data GeneratePackageDeps = GeneratePackageDeps
    deriving (Eq, Show, Typeable, Generic)
instance Binary   GeneratePackageDeps
instance Hashable GeneratePackageDeps
instance NFData   GeneratePackageDeps

data GeneratePackageMap = GeneratePackageMap
    deriving (Eq, Show, Typeable, Generic)
instance Binary   GeneratePackageMap
instance Hashable GeneratePackageMap
instance NFData   GeneratePackageMap

data RunScenarios = RunScenarios
    deriving (Eq, Show, Typeable, Generic)
instance Binary   RunScenarios
instance Hashable RunScenarios
instance NFData   RunScenarios

data EncodeModule = EncodeModule
    deriving (Eq, Show, Typeable, Generic)
instance Binary   EncodeModule
instance Hashable EncodeModule
instance NFData   EncodeModule

data CreateScenarioContext = CreateScenarioContext
    deriving (Eq, Show, Typeable, Generic)
instance Binary   CreateScenarioContext
instance Hashable CreateScenarioContext
instance NFData   CreateScenarioContext
