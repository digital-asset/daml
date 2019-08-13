-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}

-- | This module extends the general Haskell rule types in
-- `Development.IDE.Core.RuleTypes` with DAML specific rule types
-- such as those for producing DAML LF.
module Development.IDE.Core.RuleTypes.Daml(
    module Development.IDE.Core.RuleTypes,
    module Development.IDE.Core.RuleTypes.Daml
    ) where

import Control.DeepSeq
import Data.Binary
import qualified Data.ByteString as BS
import Data.Hashable
import Data.Map.Strict (Map)
import Data.Set (Set)
import Data.Typeable (Typeable)
import Development.Shake
import GHC.Generics (Generic)
import "ghc-lib-parser" Module (UnitId)
import Development.IDE.Core.Service.Daml

import Development.IDE.Types.Location
import Development.IDE.Core.RuleTypes

import DA.Daml.DocTest
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.ScenarioServiceClient as SS

import Language.Haskell.HLint4

type instance RuleResult GenerateDalf = LF.Module
type instance RuleResult GenerateRawDalf = LF.Module

-- | A newtype wrapper for LF.Package that does not force the modules
-- in the package to be evaluated to NF. This is useful since we
-- already force the evaluation when we build the modules
-- and avoids having to traverse all dependencies of a module
-- if only that module changed.
newtype WhnfPackage = WhnfPackage { getWhnfPackage :: LF.Package }
    deriving Show

instance NFData WhnfPackage where
    rnf (WhnfPackage (LF.Package ver modules)) =
        modules `seq` rnf ver

type instance RuleResult GeneratePackage = WhnfPackage
type instance RuleResult GenerateRawPackage = WhnfPackage
type instance RuleResult GeneratePackageDeps = WhnfPackage

data DalfPackage = DalfPackage
    { dalfPackageId :: LF.PackageId
    , dalfPackagePkg :: LF.ExternalPackage
    , dalfPackageBytes :: BS.ByteString
    } deriving (Show, Eq, Generic)

instance NFData DalfPackage

type instance RuleResult GeneratePackageMap = Map UnitId DalfPackage

-- | Runs all scenarios in the given file (but not scenarios in imports).
type instance RuleResult RunScenarios = [(VirtualResource, Either SS.Error SS.ScenarioResult)]

-- | Encode a module and produce a hash of the module and all its transitive dependencies.
-- The hash is used to decide if a module needs to be reloaded in the scenario service.
type instance RuleResult EncodeModule = (SS.Hash, BS.ByteString)

-- | Create a scenario context for a given module. This context is valid both for the module
-- itself but also for all of its transitive dependencies.
type instance RuleResult CreateScenarioContext = SS.ContextId

-- ^ A map from a file A to a file B whose scenario context should be
-- used for executing scenarios in A. We use this when running the scenarios
-- in transitive dependencies of the files of interest so that we only need
-- one scenario context per file of interest.
type instance RuleResult GetScenarioRoots = Map NormalizedFilePath NormalizedFilePath

-- ^ The root for the given file based on GetScenarioRoots.
-- This is a separate rule so we can avoid rerunning scenarios if
-- only the roots of other files have changed.
type instance RuleResult GetScenarioRoot = NormalizedFilePath

-- | These rules manage access to the global state in
-- envOfInterestVar and envOpenVirtualResources.
type instance RuleResult GetOpenVirtualResources = Set VirtualResource

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

data GetScenarioRoots = GetScenarioRoots
    deriving (Eq, Show, Typeable, Generic)
instance Hashable GetScenarioRoots
instance NFData   GetScenarioRoots

data GetScenarioRoot = GetScenarioRoot
    deriving (Eq, Show, Typeable, Generic)
instance Hashable GetScenarioRoot
instance NFData   GetScenarioRoot

data GetOpenVirtualResources = GetOpenVirtualResources
    deriving (Eq, Show, Typeable, Generic)
instance Hashable GetOpenVirtualResources
instance NFData   GetOpenVirtualResources

data GetDlintSettings = GetDlintSettings
    deriving (Eq, Show, Typeable, Generic)
instance Hashable GetDlintSettings
instance NFData   GetDlintSettings
instance NFData Hint where rnf = rwhnf
instance NFData Classify where rnf = rwhnf
instance Show Hint where show = const "<hint>"

type instance RuleResult GetDlintSettings = ([Classify], Hint)

data GetDlintDiagnostics = GetDlintDiagnostics
    deriving (Eq, Show, Typeable, Generic)
instance Hashable GetDlintDiagnostics
instance NFData   GetDlintDiagnostics

type instance RuleResult GetDlintDiagnostics = ()

data GenerateDocTestModule = GenerateDocTestModule
    deriving (Eq, Show, Typeable, Generic)
instance Hashable GenerateDocTestModule
instance NFData   GenerateDocTestModule

-- | File path of the generated module
type instance RuleResult GenerateDocTestModule = GeneratedModule

-- | Kick off things
type instance RuleResult OfInterest = ()

data OfInterest = OfInterest
    deriving (Eq, Show, Typeable, Generic)
instance Hashable OfInterest
instance NFData   OfInterest
