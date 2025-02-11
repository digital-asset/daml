-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker.WarnInvalidDependencies
  ( checkPackage
  ) where

import Control.Lens (set)
import Control.Monad (forM_, when)
import Control.Monad.Reader (withReaderT)
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.TypeChecker.Env
import DA.Daml.LF.TypeChecker.Error
import DA.Daml.LF.TypeChecker.Upgrade (UpgradedPkgWithNameAndVersion (..), UpgradeInfo (..))
import Data.List (nub, sortOn)
import qualified Data.Map as Map
import Development.IDE.Types.Diagnostics

{- HLINT ignore "Use nubOrd" -}
checkPackage
  :: LF.Package
  -> [LF.DalfPackage]
  -> LF.Version
  -> UpgradeInfo
  -> DamlWarningFlags ErrorOrWarning
  -> [LF.DalfPackage]
  -> Maybe (UpgradedPkgWithNameAndVersion, [UpgradedPkgWithNameAndVersion])
  -> [Diagnostic]
checkPackage pkg deps version upgradeInfo flags rootDeps mbUpgradedPackage = 
  case runGamma (LF.initWorldSelf (LF.dalfPackagePkg <$> deps) pkg) version $ withReaderT (set damlWarningFlags flags) check of
    Left err -> [toDiagnostic err]
    Right ((), warnings) -> map toDiagnostic (nub warnings)
  where
    usedPackageIdMetas :: Map.Map LF.PackageId LF.PackageMetadata
    usedPackageIdMetas = Map.fromList $ dalfPackageToMetadataTuple <$> deps
    dalfPackageToMetadataTuple :: LF.DalfPackage -> (LF.PackageId, LF.PackageMetadata)
    dalfPackageToMetadataTuple dep = (LF.dalfPackageId dep, LF.packageMetadata $ LF.extPackagePkg $ LF.dalfPackagePkg dep)
  
    check :: TcM ()
    check = do
      -- Unused dependency check
      let unusedPkgs =
            [ dalfPackageToMetadataTuple rootDep
            | rootDep <- rootDeps
            , Map.notMember (LF.dalfPackageId rootDep) usedPackageIdMetas
            ]

      when (not $ null unusedPkgs) $ diagnosticWithContext $ WEUnusedDependency $ sortOn (LF.packageName . snd) unusedPkgs
      -- Depending on upgraded package check
      when (uiTypecheckUpgrades upgradeInfo) $
        case mbUpgradedPackage of
          Just (UpgradedPkgWithNameAndVersion pkgId _ _ _, _) -> do
            let allPackageIdMetas = Map.fromList unusedPkgs <> usedPackageIdMetas
            forM_ (Map.lookup pkgId allPackageIdMetas) $ \meta ->
              diagnosticWithContext $ WEOwnUpgradeDependency (pkgId, meta)
          _ -> pure ()
      -- Creating a template when depending on daml-script check
      let definesTemplateOrInterface =
            any (\mod ->
              not $ null (LF.moduleTemplates mod) && null (LF.moduleInterfaces mod)
            ) $ LF.packageModules pkg
          damlScriptDeps = filter
            ( flip elem ["daml-script", "daml3-script", "daml-script-lts", "daml-script-lts-stable"]
            . LF.unPackageName
            . LF.packageName
            . LF.packageMetadata
            . LF.extPackagePkg
            ) $ LF.dalfPackagePkg <$> deps
      when (definesTemplateOrInterface && not (null damlScriptDeps)) $ do
        let pkgId = LF.extPackageId $ head damlScriptDeps
            pkgMeta = LF.packageMetadata $ LF.extPackagePkg $ head damlScriptDeps
        diagnosticWithContext $ WETemplateInterfaceDependsOnScript (pkgId, pkgMeta)
