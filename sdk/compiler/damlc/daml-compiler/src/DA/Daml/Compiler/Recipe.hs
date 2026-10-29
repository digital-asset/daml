-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | The build recipe damlc embeds in every DAR it writes;
-- it reflects what damlc actually resolved
module DA.Daml.Compiler.Recipe
    ( Recipe(..)
    , Subject(..)
    , Dependency(..)
    , DependencyField(..)
    , Resolved(..)
    , buildRecipe
    , encodeRecipe
    , recipeEntryPath
    ) where

import Control.Monad (forM)
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Package.Config (PackageConfigFields(..))
import qualified Data.Aeson as A
import qualified Data.ByteString.Lazy as BSL
import Data.List (isPrefixOf)
import Data.List.Extra (nubOrd)
import qualified Data.Map.Strict as Map
import Data.Map.Strict (Map)
import Data.Maybe (isNothing)
import qualified Data.Text as T
import qualified Module as Ghc
import System.Directory (doesFileExist)
import System.FilePath ((</>))

-- | Which of daml.yaml's two dependency lists a dependency came from.
data DependencyField = FieldDependencies | FieldDataDependencies

instance A.ToJSON DependencyField where
    toJSON FieldDependencies = A.String "dependencies"
    toJSON FieldDataDependencies = A.String "data-dependencies"

-- | resolved, non-SDK dependency
data Dependency = Dependency
    { depNameVersion :: T.Text
    , depPackageId :: LF.PackageId
    , depField :: DependencyField
    }

instance A.ToJSON Dependency where
    toJSON d = A.object
        [ "name-version" A..= depNameVersion d
        , "package-id" A..= LF.unPackageId (depPackageId d)
        , "field" A..= depField d
        ]

-- | Names the artifact, so a recipe that does not correspond to the DAR at
-- hand is caught before anything is compiled.
data Subject = Subject
    { subjPackageId :: LF.PackageId
    , subjName :: LF.PackageName
    , subjVersion :: Maybe LF.PackageVersion
    }

instance A.ToJSON Subject where
    toJSON s = A.object $
        [ "package-id" A..= LF.unPackageId (subjPackageId s)
        , "name" A..= LF.unPackageName (subjName s)
        ]
        ++ [ "version" A..= LF.unPackageVersion v | Just v <- [subjVersion s] ]

data Resolved = Resolved
    { resolvedLfTarget :: T.Text
    , resolvedIncremental :: Bool
    }

instance A.ToJSON Resolved where
    toJSON r = A.object
        [ "lf-target" A..= resolvedLfTarget r
        , "incremental" A..= resolvedIncremental r
        ]

data Recipe = Recipe
    { recipeSubject :: Subject
    , recipeDamlcVersion :: T.Text
    , recipeSource :: T.Text
    , recipeBuildOptions :: [T.Text]
    , recipeUpgrades :: Maybe LF.PackageId
    , recipeDependencies :: [Dependency]
    , recipeSdkDependencies :: [T.Text]
    , recipeModulePrefixes :: Map T.Text T.Text
    , recipeResolved :: Resolved
    }

instance A.ToJSON Recipe where
    toJSON r = A.object $
        [ "schema" A..= ("daml-build-recipe/v1" :: T.Text)
        , "subject" A..= recipeSubject r
        , "damlc-version" A..= recipeDamlcVersion r
        , "source" A..= recipeSource r
        , "build-options" A..= recipeBuildOptions r
        , "dependencies" A..= recipeDependencies r
        , "resolved" A..= recipeResolved r
        ]
        ++ [ "upgrades" A..= LF.unPackageId u | Just u <- [recipeUpgrades r] ]
        ++ [ "sdk-dependencies" A..= recipeSdkDependencies r | not (null (recipeSdkDependencies r)) ]
        ++ [ "module-prefixes" A..= recipeModulePrefixes r | not (Map.null (recipeModulePrefixes r)) ]

encodeRecipe :: Recipe -> BSL.ByteString
encodeRecipe = A.encode

-- | The fixed path the recipe is written at inside every DAR, alongside
-- META-INF/MANIFEST.MF 
recipeEntryPath :: FilePath
recipeEntryPath = "META-INF/build-recipe.json"

buildRecipe
    :: PackageConfigFields
    -> T.Text -- ^ damlc's own version, the same string Sdk-Version already carries.
    -> LF.Version
    -> Bool -- ^ whether this is an incremental build (isJust . optShakeFiles of the IdeOptions in scope, not damlc's own Options -- buildDar does not have the latter).
    -> LF.PackageId
    -> Maybe LF.PackageId -- ^ the predecessor package id, if this is an upgrade.
    -> Map Ghc.UnitId LF.DalfPackage -- ^ unstableDeps.
    -> FilePath -- ^ The .daml/dependencies directory DA.Cli.Damlc.DependencyDb already populated for this build, used by resolveDependencyField.
    -> IO Recipe
buildRecipe fields damlcVersion lfVersion isIncremental pkgId upgradedPkgId unstableDeps depsDir = do
    recipeDependencies <-
        forM
            [ (unitId, dalfPkg)
            | (unitId, dalfPkg) <- Map.toList unstableDeps
            , isNothing (sdkPackageName (Ghc.unitIdString unitId))
            ]
            (\(unitId, dalfPkg) -> do
                depField <- resolveDependencyField depsDir (LF.dalfPackageId dalfPkg)
                pure Dependency
                    { depNameVersion = T.pack (Ghc.unitIdString unitId)
                    , depPackageId = LF.dalfPackageId dalfPkg
                    , depField = depField
                    })
    pure Recipe
        { recipeSubject = Subject
            { subjPackageId = pkgId
            , subjName = pName fields
            , subjVersion = pVersion fields
            }
        , recipeDamlcVersion = damlcVersion
        , recipeSource = T.pack (pSrc fields)
        , recipeBuildOptions = map T.pack (pBuildOptions fields)
        , recipeUpgrades = upgradedPkgId
        , recipeDependencies = recipeDependencies
        , recipeSdkDependencies =
            nubOrd
            [ T.pack name
            | unitId <- Map.keys unstableDeps
            , Just name <- [sdkPackageName (Ghc.unitIdString unitId)]
            ]
        , recipeModulePrefixes =
            Map.fromList
                [ (T.pack (Ghc.unitIdString unitId), T.pack (Ghc.moduleNameString modName))
                | (unitId, modName) <- Map.toList (pModulePrefixes fields)
                ]
        , recipeResolved = Resolved
            { resolvedLfTarget = T.pack (LF.renderVersion lfVersion)
            , resolvedIncremental = isIncremental
            }
        }
-- sdkPackageName and dependencyMarkerFile are defined as literals here rather than imported, because this library
-- (daml-compiler) is itself a dependency of the damlc library that
-- DependencyDb lives in -- importing it back would be a module cycle.
--
-- Not a definitive list; no single authoritative source exists for this.
sdkPackageName :: String -> Maybe String
sdkPackageName unitIdStr
    | "daml-prim" `isPrefixOf` unitIdStr = Just "daml-prim"
    | "daml-stdlib" `isPrefixOf` unitIdStr = Just "daml-stdlib"
    | "daml-script" `isPrefixOf` unitIdStr = Just "daml-script"
    | "daml3-script" `isPrefixOf` unitIdStr = Just "daml3-script"
    | "daml-trigger" `isPrefixOf` unitIdStr = Just "daml-trigger"
    | otherwise = Nothing


dependencyMarkerFile :: FilePath
dependencyMarkerFile = "_pkg_"

-- | Which daml.yaml field a resolved dependency actually came from. Read
-- back from the marker file DA.Cli.Damlc.DependencyDb already wrote when
-- installing it 
resolveDependencyField :: FilePath -> LF.PackageId -> IO DependencyField
resolveDependencyField depsDir pkgId = do
    isDep <- doesFileExist (depsDir </> T.unpack (LF.unPackageId pkgId) </> dependencyMarkerFile)
    pure $ if isDep then FieldDependencies else FieldDataDependencies
