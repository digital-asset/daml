-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}

-- | This module extends the general Haskell rule types in
-- `Development.IDE.Core.RuleTypes` with Daml specific rule types
-- such as those for producing Daml-LF.
module Development.IDE.Core.RuleTypes.Daml(
    module Development.IDE.Core.RuleTypes,
    module Development.IDE.Core.RuleTypes.Daml
    ) where

import "ghc-lib-parser" Module (UnitId)
import Control.DeepSeq
import DA.Daml.DocTest
import DA.Daml.LF.Ast qualified as LF
import DA.Daml.LF.ScenarioServiceClient qualified as SS
import Data.Binary
import Data.ByteString qualified as BS
import Data.HashSet (HashSet)
import Data.Hashable
import Data.Map.Strict (Map)
import Data.Text qualified as T
import Data.Typeable (Typeable)
import Development.IDE.Core.RuleTypes
import Development.IDE.GHC.Util
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import Development.Shake
import GHC.Generics (Generic)
import HscTypes (ModIface, ModSummary)
import Language.Haskell.HLint4

type instance RuleResult GenerateDalf = LF.Module
type instance RuleResult GenerateSerializedDalf = ()
type instance RuleResult GenerateRawDalf = LF.Module

-- | A newtype wrapper for LF.Package that does not force the modules
-- in the package to be evaluated to NF. This is useful since we
-- already force the evaluation when we build the modules
-- and avoids having to traverse all dependencies of a module
-- if only that module changed.
newtype WhnfPackage = WhnfPackage { getWhnfPackage :: LF.Package }
    deriving Show

instance NFData WhnfPackage where
    rnf (WhnfPackage (LF.Package ver modules metadata)) =
        modules `seq` rnf ver `seq` rnf metadata

type instance RuleResult GeneratePackage = WhnfPackage
type instance RuleResult GenerateRawPackage = WhnfPackage
type instance RuleResult GeneratePackageDeps = WhnfPackage

newtype GeneratePackageMapFun = GeneratePackageMapFun (FilePath -> Action ([FileDiagnostic], Map UnitId LF.DalfPackage))
instance Show GeneratePackageMapFun where show _ = "GeneratePackageMapFun"
instance NFData GeneratePackageMapFun where rnf !_ = ()
-- | This matches how GhcSessionIO is handled in ghcide.
type instance RuleResult GeneratePackageMapIO = GeneratePackageMapFun

-- | We only want to force this to WHNF not to NF since that is way too slow,
-- repeated for each file and unnecessary since decoding forces it.
newtype PackageMap = PackageMap { getPackageMap :: Map UnitId LF.DalfPackage }
  deriving Show
instance NFData PackageMap where
    rnf = rwhnf
type instance RuleResult GeneratePackageMap = PackageMap
type instance RuleResult GenerateStablePackages = Map (UnitId, LF.ModuleName) LF.DalfPackage

data DamlGhcSession = DamlGhcSession (Maybe NormalizedFilePath)
    deriving (Show, Eq, Generic)
instance Binary DamlGhcSession
instance Hashable DamlGhcSession
instance NFData DamlGhcSession
type instance RuleResult DamlGhcSession = HscEnvEq

-- | Virtual resources
data VirtualResource = VRScenario
    { vrScenarioFile :: !NormalizedFilePath
    , vrScenarioName :: !T.Text
    } deriving (Eq, Ord, Show, Generic)
    -- VRScenario identifies a scenario in a given file.
    -- This virtual resource is associated with the HTML result of
    -- interpreting the corresponding scenario.

instance Hashable VirtualResource
instance NFData VirtualResource

-- | Runs all scenarios in the given file (but not scenarios in imports).
type instance RuleResult RunScenarios = [(VirtualResource, Either SS.Error SS.ScenarioResult)]

-- | Runs a single scenarios in the given file with the given name
-- Kept as a list as we can't enforce a single test as an invariant - there may
-- be no test with that name
type instance RuleResult RunSingleScenario = [(VirtualResource, Either SS.Error SS.ScenarioResult)]

type instance RuleResult GetScenarios = [VirtualResource]

type instance RuleResult RunScripts = [(VirtualResource, Either SS.Error SS.ScenarioResult)]

type instance RuleResult RunSingleScript = [(VirtualResource, Either SS.Error SS.ScenarioResult)]

type instance RuleResult GetScripts = [VirtualResource]

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
type instance RuleResult GetOpenVirtualResources = HashSet VirtualResource

-- | This is used for on-disk incremental builds
type instance RuleResult ReadSerializedDalf = LF.Module
-- | Read the interface and the summary. This rule has a cutoff on the ABI hash of the interface.
-- It is important to get the modsummary from this rule rather than depending on the parsed module.
-- Otherwise you will always recompile module B if B depends on A and A changed even if A’s ABI stayed
-- the same
type instance RuleResult ReadInterface = (ModSummary, ModIface)

instance Show ModIface where
    show _ = "<ModIface>"

instance NFData ModIface where
    rnf = rwhnf

data GenerateDalf = GenerateDalf
    deriving (Eq, Show, Typeable, Generic)
instance Binary   GenerateDalf
instance Hashable GenerateDalf
instance NFData   GenerateDalf

data GenerateSerializedDalf = GenerateSerializedDalf
    deriving (Eq, Show, Typeable, Generic)
instance Binary GenerateSerializedDalf
instance Hashable GenerateSerializedDalf
instance NFData GenerateSerializedDalf

data ReadSerializedDalf = ReadSerializedDalf
    deriving (Eq, Show, Typeable, Generic)
instance Binary   ReadSerializedDalf
instance Hashable ReadSerializedDalf
instance NFData   ReadSerializedDalf

data ReadInterface = ReadInterface
    deriving (Eq, Show, Typeable, Generic)
instance Binary   ReadInterface
instance Hashable ReadInterface
instance NFData   ReadInterface

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

data GeneratePackageMapIO = GeneratePackageMapIO deriving (Eq, Show, Typeable, Generic)
instance Hashable GeneratePackageMapIO
instance NFData   GeneratePackageMapIO
instance Binary   GeneratePackageMapIO

data GenerateStablePackages = GenerateStablePackages
   deriving (Eq, Show, Typeable, Generic)
instance Binary GenerateStablePackages
instance Hashable GenerateStablePackages
instance NFData GenerateStablePackages

data RunScenarios = RunScenarios
    deriving (Eq, Show, Typeable, Generic)
instance Binary   RunScenarios
instance Hashable RunScenarios
instance NFData   RunScenarios

data RunSingleScenario = RunSingleScenario T.Text
    deriving (Eq, Show, Typeable, Generic)
instance Binary   RunSingleScenario
instance Hashable RunSingleScenario
instance NFData   RunSingleScenario

data GetScenarios = GetScenarios
    deriving (Eq, Show, Typeable, Generic)
instance Binary   GetScenarios
instance Hashable GetScenarios
instance NFData   GetScenarios

data RunScripts = RunScripts
    deriving (Eq, Show, Typeable, Generic)
instance Binary   RunScripts
instance Hashable RunScripts
instance NFData   RunScripts

data RunSingleScript = RunSingleScript T.Text
    deriving (Eq, Show, Typeable, Generic)
instance Binary   RunSingleScript
instance Hashable RunSingleScript
instance NFData   RunSingleScript

data GetScripts = GetScripts
    deriving (Eq, Show, Typeable, Generic)
instance Binary   GetScripts
instance Hashable GetScripts
instance NFData   GetScripts

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
instance Binary   GetScenarioRoots

data GetScenarioRoot = GetScenarioRoot
    deriving (Eq, Show, Typeable, Generic)
instance Hashable GetScenarioRoot
instance NFData   GetScenarioRoot
instance Binary   GetScenarioRoot

data GetOpenVirtualResources = GetOpenVirtualResources
    deriving (Eq, Show, Typeable, Generic)
instance Hashable GetOpenVirtualResources
instance NFData   GetOpenVirtualResources
instance Binary   GetOpenVirtualResources

data GetDlintSettings = GetDlintSettings
    deriving (Eq, Show, Typeable, Generic)
instance Hashable GetDlintSettings
instance NFData   GetDlintSettings
instance NFData Hint where rnf = rwhnf
instance NFData Classify where rnf = rwhnf
instance Show Hint where show = const "<hint>"
instance Binary GetDlintSettings

type instance RuleResult GetDlintSettings = ([Classify], Hint)

data GetDlintDiagnostics = GetDlintDiagnostics
    deriving (Eq, Show, Typeable, Generic)
instance Hashable GetDlintDiagnostics
instance NFData   GetDlintDiagnostics
instance Binary   GetDlintDiagnostics

type instance RuleResult GetDlintDiagnostics = ()

data GenerateDocTestModule = GenerateDocTestModule
    deriving (Eq, Show, Typeable, Generic)
instance Hashable GenerateDocTestModule
instance NFData   GenerateDocTestModule
instance Binary   GenerateDocTestModule

-- | File path of the generated module
type instance RuleResult GenerateDocTestModule = GeneratedModule

-- | Kick off things
type instance RuleResult OfInterest = ()

data OfInterest = OfInterest
    deriving (Eq, Show, Typeable, Generic)
instance Hashable OfInterest
instance NFData   OfInterest
instance Binary   OfInterest
