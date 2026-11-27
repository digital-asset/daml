-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | The @daml.yaml@ damlc embeds in every DAR that
-- records only what building the package again needs.
module DA.Daml.Compiler.EmbeddedDamlYaml
    ( EmbeddedDamlYaml(..)
    , embeddedDamlYamlValue
    , embeddedDamlYamlEntryPath
    , renderEmbeddedDamlYaml
    , relativeSource
    , dataDependencies
    , packageExposureFlags
    , upgradeCheckOptions
    ) where

import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Package.Config ()
import DA.Daml.Resolution.Config (basePackages)
import qualified Data.Aeson as A
import qualified Data.ByteString.Lazy as BSL
import Data.List (elemIndex)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe, isJust, mapMaybe)
import Data.Ord (comparing)
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Yaml.Pretty as Yaml
import qualified Module as Ghc
import System.FilePath (isAbsolute, makeRelative, normalise, splitDirectories)

embeddedDamlYamlEntryPath :: FilePath
embeddedDamlYamlEntryPath = "daml.yaml"

data EmbeddedDamlYaml = EmbeddedDamlYaml
    { edyName :: LF.PackageName
    , edyVersion :: Maybe LF.PackageVersion
    , edyDamlcVersion :: T.Text
      -- ^ Recorded as @components: [damlc:\<version\>]@.
    , edySource :: FilePath
      -- ^ Relative to the package root. See 'relativeSource'.
    , edyExposedModules :: Maybe [Ghc.ModuleName]
      -- ^ Exposed modules, duplicating those carried in the conf
    , edyDataDependencies :: [Ghc.UnitId]
      -- ^ The DALFs in this DAR the package was built against
    , edyUpgrades :: Maybe LF.PackageId
      -- ^ The id of the package this one upgrades, when it is an upgrade.
    , edyModulePrefixes :: Map Ghc.UnitId Ghc.ModuleName
      -- ^ Does not change the package id, but a build fails without it.
    , edyBuildOptions :: [T.Text]
      -- ^ daml.yaml's @build-options:@ verbatim
      -- TODO: consider stripping options files are written
      -- at arbitrary locations on the user's machine
      -- outside the pkg directory (or maybe some other solution like
      -- building in a container?).
    }

-- | The file's JSON shape, given where in the archive each carried DALF sits.
embeddedDamlYamlValue :: Map Ghc.UnitId FilePath -> EmbeddedDamlYaml -> A.Value
embeddedDamlYamlValue dalfPaths EmbeddedDamlYaml {..} = A.object $
        [ "components" A..= [componentRef "damlc" edyDamlcVersion]
        , "name" A..= LF.unPackageName edyName
        , "dependencies" A..= ([] :: [T.Text])
        , "source" A..= T.pack edySource
        , "environment-variable-interpolation" A..= False
        ]
        ++ [ "version" A..= LF.unPackageVersion version | Just version <- [edyVersion] ]
        ++ [ "upgrades" A..= LF.unPackageId predecessor | Just predecessor <- [edyUpgrades] ]
        ++ [ "exposed-modules" A..= exposed | Just exposed <- [edyExposedModules] ]
        ++ [ "data-dependencies" A..= dataDependencyPaths | not (null dataDependencyPaths) ]
        ++ [ "module-prefixes" A..= edyModulePrefixes | not (Map.null edyModulePrefixes) ]
        ++ [ "build-options" A..= edyBuildOptions | not (null edyBuildOptions) ]
      where
        componentRef name version = name <> ":" <> version
        dataDependencyPaths = mapMaybe (`Map.lookup` dalfPaths) edyDataDependencies

renderEmbeddedDamlYaml :: Map Ghc.UnitId FilePath -> EmbeddedDamlYaml -> BSL.ByteString
renderEmbeddedDamlYaml dalfPaths =
    BSL.fromStrict . Yaml.encodePretty config . embeddedDamlYamlValue dalfPaths
  where
    config = Yaml.setConfCompare (comparing fieldPosition) Yaml.defConfig
    fieldPosition field = fromMaybe (length fieldOrder) (elemIndex field fieldOrder)
    fieldOrder =
        [ "components"
        , "name"
        , "version"
        , "source"
        , "exposed-modules"
        , "dependencies"
        , "data-dependencies"
        , "upgrades"
        , "module-prefixes"
        , "build-options"
        , "environment-variable-interpolation"
        ]

-- | Where the package's source sits relative to the package root;
-- return Nothing if it resolves outside that root.
relativeSource :: FilePath -> FilePath -> Maybe FilePath
relativeSource packageRoot source
    | escapes relative = Nothing
    | null relative = Just "."
    | otherwise = Just relative
  where
    relative = normalise (makeRelative (normalise packageRoot) (normalise source))
    escapes path = isAbsolute path || ".." `elem` splitDirectories path

-- | The DALFs a build from this file points at: everything this DAR
-- carries, bar the base packages damlc ships.
dataDependencies :: [Ghc.UnitId] -> [Ghc.UnitId]
dataDependencies carried =
    [ unitId
    | unitId <- carried
    , fst (LF.splitUnitId unitId) `notElem` compilerPackages
    ]
  where
    compilerPackages = map (LF.PackageName . T.pack) basePackages

-- | Build-options exposing the direct DALF dependencies, which otherwise
-- remain hidden and fail to resolve on import.
packageExposureFlags
    :: [Ghc.UnitId]
       -- ^ The package's direct dependencies.
    -> [Ghc.UnitId]
       -- ^ What a build from this file is pointed at, from 'dataDependencies'.
    -> [T.Text]
packageExposureFlags directDependencies pointedAt =
    [ "--package=" <> T.pack (Ghc.unitIdString unitId)
    | unitId <- directDependencies
    , unitId `Set.member` carried
    ]
  where
    carried = Set.fromList pointedAt
upgradeCheckOptions :: Maybe LF.PackageId -> [T.Text]
upgradeCheckOptions predecessor = ["--typecheck-upgrades=no" | isJust predecessor]
