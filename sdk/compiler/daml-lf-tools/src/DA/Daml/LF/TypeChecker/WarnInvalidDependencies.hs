-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker.WarnInvalidDependencies
  ( checkPackage
  ) where

import Control.Lens (set)
import Control.Monad (guard, when)
import Control.Monad.Reader (withReaderT)
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.TypeChecker.Env
import DA.Daml.LF.TypeChecker.Error
import DA.Daml.LF.TypeChecker.Upgrade (UpgradedPkgWithNameAndVersion (..), UpgradeInfo (..))
import Data.Bifunctor (bimap)
import Data.Maybe (mapMaybe)
import Data.List (intercalate, nub, sort)
import Data.List.Extra (unsnoc)
import Data.List.Split(splitOn)
import qualified Data.Set as Set
import qualified Data.Text as T
import Development.IDE.Types.Diagnostics
import "ghc-lib-parser" Module

{- HLINT ignore "Use nubOrd" -}
checkPackage
  :: LF.Package
  -> [LF.DalfPackage]
  -> LF.Version
  -> UpgradeInfo
  -> DamlWarningFlags ErrorOrWarning
  -> [UnitId]
  -> Maybe (UpgradedPkgWithNameAndVersion, [UpgradedPkgWithNameAndVersion])
  -> [Diagnostic]
checkPackage pkg deps version upgradeInfo flags rootDependencies mbUpgradedPackage = 
  case runGamma (LF.initWorldSelf (LF.dalfPackagePkg <$> deps) pkg) version $ withReaderT (set damlWarningFlags flags) check of
    Left err -> [toDiagnostic err]
    Right ((), warnings) -> map toDiagnostic (nub warnings)
  where
    metaToUnitId :: LF.PackageMetadata -> String
    metaToUnitId LF.PackageMetadata {..} = T.unpack $ LF.unPackageName packageName <> "-" <> LF.unPackageVersion packageVersion
    usedUnitIds :: Set.Set UnitId
    usedUnitIds = Set.fromList $ stringToUnitId . metaToUnitId . LF.packageMetadata . LF.extPackagePkg . LF.dalfPackagePkg <$> deps
    check :: TcM ()
    check = do
      -- Unused dependency check
      let unusedPkgs = flip mapMaybe rootDependencies $ \rootDepUnitId -> do
            guard $ Set.notMember rootDepUnitId usedUnitIds
            bimap (LF.PackageName . T.pack . intercalate "-") (LF.PackageVersion . T.pack) <$> unsnoc (splitOn "-" $ unitIdString rootDepUnitId)
      when (not $ null unusedPkgs) $ diagnosticWithContext $ WEUnusedDependency $ sort unusedPkgs
      -- Depending on upgraded package check
      when (uiTypecheckUpgrades upgradeInfo) $
        case mbUpgradedPackage of
          Just (UpgradedPkgWithNameAndVersion _ _ pkgName (Just pkgVersion), _) -> do
            let allUnitIds = Set.fromList rootDependencies <> usedUnitIds
            when (Set.member (stringToUnitId $ T.unpack $ LF.unPackageName pkgName <> "-" <> LF.unPackageVersion pkgVersion) allUnitIds) $
              diagnosticWithContext $ WEOwnUpgradeDependency pkgName pkgVersion
          _ -> pure ()
