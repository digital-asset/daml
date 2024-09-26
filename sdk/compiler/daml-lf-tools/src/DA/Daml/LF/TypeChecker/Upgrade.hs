-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}

module DA.Daml.LF.TypeChecker.Upgrade (
        module DA.Daml.LF.TypeChecker.Upgrade
    ) where

import           Control.Monad (unless, forM, forM_, when)
import           Control.Monad.Extra (unlessM)
import           Control.Monad.Reader (withReaderT, ask)
import           Control.Monad.Reader.Class (asks)
import           Control.Lens hiding (Context)
import           DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Ast.Alpha as Alpha
import           DA.Daml.LF.TypeChecker.Check (expandTypeSynonyms)
import           DA.Daml.LF.TypeChecker.Env
import           DA.Daml.LF.TypeChecker.Error
import           DA.Daml.Options.Types (UpgradeInfo (..))
import           Data.Bifunctor (first)
import           Data.Either (partitionEithers)
import           Data.Hashable
import qualified Data.HashMap.Strict as HMS
import           Data.List (foldl', nub)
import qualified Data.NameMap as NM
import qualified Data.Text as T
import           Development.IDE.Types.Diagnostics
import Data.Maybe (catMaybes, mapMaybe, isNothing)
import Safe (maximumByMay, minimumByMay)
import Data.Function (on)
import Module (UnitId)

-- Allows us to split the world into upgraded and non-upgraded
type TcUpgradeM = TcMF UpgradingEnv
type TcPreUpgradeM = TcMF (Version, UpgradeInfo)

type DepsMap = HMS.HashMap LF.PackageId UpgradingDep

type UpgradeablePackage = (Maybe RawPackageVersion, LF.PackageId, LF.Package)
type UpgradeablePackageMap = HMS.HashMap LF.PackageName [UpgradeablePackage]

data UpgradingEnv = UpgradingEnv
  { _upgradingGamma :: Upgrading Gamma
  , _depsMap :: DepsMap
  }

makeLenses ''UpgradingEnv

present' :: Functor f => (Gamma -> f Gamma) -> UpgradingEnv -> f UpgradingEnv
present' = upgradingGamma . present

runGammaUnderUpgrades :: Upgrading (TcM a) -> TcUpgradeM (Upgrading a)
runGammaUnderUpgrades Upgrading{ _past = pastAction, _present = presentAction } = do
    pastResult <- withReaderT (_past . _upgradingGamma) pastAction
    presentResult <- withReaderT (_present . _upgradingGamma) presentAction
    pure Upgrading { _past = pastResult, _present = presentResult }

shouldTypecheck :: Version -> UpgradeInfo -> Bool
shouldTypecheck version upgradeInfo = version `LF.supports` LF.featurePackageUpgrades && uiTypecheckUpgrades upgradeInfo

shouldTypecheckM :: TcPreUpgradeM Bool
shouldTypecheckM = asks (uncurry shouldTypecheck)

mkGamma :: Version -> UpgradeInfo -> World -> Gamma
mkGamma version upgradeInfo world =
    let addBadIfaceSwapIndicator :: Gamma -> Gamma
        addBadIfaceSwapIndicator =
            if uiWarnBadInterfaceInstances upgradeInfo
            then
                addDiagnosticSwapIndicator (\case
                    Left WEUpgradeShouldDefineIfaceWithoutImplementation {} -> Just True
                    Left WEUpgradeShouldDefineTplInSeparatePackage {} -> Just True
                    Left WEUpgradeShouldDefineIfacesAndTemplatesSeparately {} -> Just True
                    _ -> Nothing)
            else id
    in
    addBadIfaceSwapIndicator $ emptyGamma world version

gammaM :: World -> TcPreUpgradeM Gamma
gammaM world = asks (flip (uncurry mkGamma) world)

{- HLINT ignore "Use nubOrd" -}
extractDiagnostics :: Version -> UpgradeInfo -> TcPreUpgradeM () -> [Diagnostic]
extractDiagnostics version upgradeInfo action =
  case runGammaF (version, upgradeInfo) action of
    Left err -> [toDiagnostic err]
    Right ((), warnings) -> map toDiagnostic (nub warnings)

checkPackage
  :: LF.Package
  -> [(UnitId, LF.DalfPackage)] -> Version -> UpgradeInfo
  -> Maybe (UpgradedPkgWithNameAndVersion, [UpgradedPkgWithNameAndVersion])
  -> [Diagnostic]
checkPackage pkg deps version upgradeInfo mbUpgradedPkg =
  extractDiagnostics version upgradeInfo $ do
    shouldTypecheck <- shouldTypecheckM
    when shouldTypecheck $ do
      case mbUpgradedPkg of
        Nothing -> pure ()
        Just (upgradedPkg@(upgradedPkgId, upgradedPkgPkg, _, _), upgradingDeps) -> do
            depsMap <- checkUpgradeDependenciesM deps (upgradedPkg : upgradingDeps)
            checkPackageBoth CheckOnlyMissingModules Nothing pkg ((upgradedPkgId, upgradedPkgPkg), depsMap)

checkPackageBoth :: CheckDepth -> Maybe Context -> LF.Package -> ((LF.PackageId, LF.Package), DepsMap) -> TcPreUpgradeM ()
checkPackageBoth checkDepth mbContext pkg ((upgradedPkgId, upgradedPkg), depsMap) =
  let presentWorld = initWorldSelf [] pkg
      pastWorld = initWorldSelf [] upgradedPkg
      upgradingWorld = Upgrading { _past = pastWorld, _present = presentWorld }
      withMbContext :: TcUpgradeM () -> TcUpgradeM ()
      withMbContext =
        case mbContext of
          Nothing -> id
          Just context -> withContextF present' context
  in
  withReaderT (\(version, upgradeInfo) -> UpgradingEnv (mkGamma version upgradeInfo <$> upgradingWorld) depsMap) $
    withMbContext $
      checkPackageM checkDepth (UpgradedPackageId upgradedPkgId) (Upgrading upgradedPkg pkg)

data CheckDepth = CheckAll | CheckOnlyMissingModules
  deriving (Show, Eq, Ord)

checkPackageSingle :: Maybe Context -> LF.Package -> TcPreUpgradeM ()
checkPackageSingle mbContext pkg =
  let presentWorld = initWorldSelf [] pkg
      withMbContext :: TcM () -> TcM ()
      withMbContext = maybe id withContext mbContext
  in
  withReaderT (\(version, upgradeInfo) -> mkGamma version upgradeInfo presentWorld) $
    withMbContext $ do
      checkNewInterfacesAreUnused pkg
      checkNewInterfacesHaveNoTemplates

type UpgradedPkgWithNameAndVersion = (LF.PackageId, LF.Package, LF.PackageName, Maybe LF.PackageVersion)

checkModule
  :: LF.World -> LF.Module
  -> [(UnitId, LF.DalfPackage)] -> Version -> UpgradeInfo
  -> Maybe (UpgradedPkgWithNameAndVersion, [UpgradedPkgWithNameAndVersion])
  -> [Diagnostic]
checkModule world0 module_ deps version upgradeInfo mbUpgradedPkg =
  extractDiagnostics version upgradeInfo $
    when (shouldTypecheck version upgradeInfo) $ do
      let world = extendWorldSelf module_ world0
      withReaderT (\(version, upgradeInfo) -> mkGamma version upgradeInfo world) $ do
        checkNewInterfacesAreUnused module_
        checkNewInterfacesHaveNoTemplates
      case mbUpgradedPkg of
        Nothing -> pure ()
        Just (upgradedPkgWithId@(upgradedPkgIdRaw, upgradedPkg, _, _), upgradingDeps) -> do
            let upgradedPkgId = UpgradedPackageId upgradedPkgIdRaw
            -- TODO: https://github.com/digital-asset/daml/issues/19859
            depsMap <- checkUpgradeDependenciesM deps (upgradedPkgWithId : upgradingDeps)
            let upgradingWorld = Upgrading { _past = initWorldSelf [] upgradedPkg, _present = world }
            withReaderT (\(version, upgradeInfo) -> UpgradingEnv (mkGamma version upgradeInfo <$> upgradingWorld) depsMap) $
              case NM.lookup (NM.name module_) (LF.packageModules upgradedPkg) of
                Nothing -> pure ()
                Just pastModule -> do
                  let upgradingModule = Upgrading { _past = pastModule, _present = module_ }
                  checkModuleM upgradedPkgId upgradingModule


checkUpgradeDependenciesM
    :: [(UnitId, LF.DalfPackage)]
    -> [UpgradedPkgWithNameAndVersion]
    -> TcPreUpgradeM DepsMap
checkUpgradeDependenciesM presentDeps pastDeps = do
    initialUpgradeablePackageMap <-
      fmap (HMS.fromListWith (<>) . catMaybes) $ forM pastDeps $ \pastDep -> do
          let (pkgId, pkg, pkgName, mbPkgVersion) = pastDep
          withPkgAsGamma pkg $
            case mbPkgVersion of
              Nothing -> do
                when (pkgSupportsUpgrades pkg && PackageName "daml-prim" /= pkgName) $
                  diagnosticWithContext $ WErrorToWarning $ WEDependencyHasNoMetadataDespiteUpgradeability pkgId UpgradedPackage
                pure $ Just (pkgName, [(Nothing, pkgId, pkg)])
              Just packageVersion -> do
                case splitPackageVersion id packageVersion of
                  Left version -> do
                    diagnosticWithContext $ WErrorToWarning $ WEDependencyHasUnparseableVersion pkgName version UpgradedPackage
                    pure Nothing
                  Right rawVersion ->
                    pure $ Just (pkgName, [(Just rawVersion, pkgId, pkg)])

    let withIdAndPkg :: (UnitId, LF.DalfPackage) -> (LF.PackageId, (UnitId, LF.DalfPackage), LF.Package)
        withIdAndPkg x@(_, dalfPkg) = (dalfPackageId dalfPkg, x, extPackagePkg (dalfPackagePkg dalfPkg))
        withoutIdAndPkg :: (LF.PackageId, (UnitId, LF.DalfPackage), LF.Package) -> (UnitId, LF.DalfPackage)
        withoutIdAndPkg (_, x, _) = x

    -- TODO: https://github.com/digital-asset/daml/issues/19859
    case topoSortPackages (map withIdAndPkg presentDeps) of
      Left badTrace -> do
        let placeholderPkg = let (_, _, pkg) = head badTrace in pkg
            getPkgIdAndMetadata (pkgId, _, pkg) = (pkgId, packageMetadata pkg)
        withPkgAsGamma placeholderPkg $
          throwWithContext $ EUpgradeDependenciesFormACycle $ map getPkgIdAndMetadata badTrace
      Right sortedPresentDeps -> do
        let dependenciesFirst = reverse (map withoutIdAndPkg sortedPresentDeps)
        upgradeablePackageMap <- checkAllDeps initialUpgradeablePackageMap dependenciesFirst
        pure $ upgradeablePackageMapToDeps upgradeablePackageMap
    where
    withPkgAsGamma :: Package -> TcM a -> TcPreUpgradeM a
    withPkgAsGamma pkg action =
      withReaderT (\(version, _) -> emptyGamma (initWorldSelf [] pkg) version) action

    upgradeablePackageMapToDeps :: UpgradeablePackageMap -> DepsMap
    upgradeablePackageMapToDeps upgradeablePackageMap =
      HMS.fromList
        [ ( pkgId
          , UpgradingDep
              { udPkgName, udMbPackageVersion
              , udIsUtilityPackage = isUtilityPackage pkg
              , udVersionSupportsUpgrades = packageLfVersion pkg `supports` featurePackageUpgrades
              , udPkgId = pkgId
              }
          )
        | (udPkgName, versions) <- HMS.toList upgradeablePackageMap
        , (udMbPackageVersion, pkgId, pkg) <- versions
        ]

    addDep
      :: (LF.PackageName, UpgradeablePackage)
      -> UpgradeablePackageMap
      -> UpgradeablePackageMap
    addDep (name, pkgVersionIdAndAst) upgradeablePackageMap =
      HMS.insertWith (<>) name [pkgVersionIdAndAst] upgradeablePackageMap

    checkAllDeps
      :: UpgradeablePackageMap
      -> [(UnitId, LF.DalfPackage)]
      -> TcPreUpgradeM UpgradeablePackageMap
    checkAllDeps upgradeablePackageMap [] = pure upgradeablePackageMap
    checkAllDeps upgradeablePackageMap (pkg:rest) = do
      mbNewDep <- checkOneDep upgradeablePackageMap pkg
      let newUpgradeablePackageMap =
            case mbNewDep of
              Nothing -> upgradeablePackageMap
              Just res -> addDep res upgradeablePackageMap
      checkAllDeps newUpgradeablePackageMap rest

    checkOneDep
      :: UpgradeablePackageMap
      -> (UnitId, LF.DalfPackage)
      -> TcPreUpgradeM (Maybe (LF.PackageName, UpgradeablePackage))
    checkOneDep upgradeablePackageMap (unitId, dalfPkg) = do
      let (packageName, mbPkgVersion) = LF.splitUnitId unitId
          LF.DalfPackage{dalfPackagePkg,dalfPackageId=presentPkgId} = dalfPkg
          presentPkg = extPackagePkg dalfPackagePkg
      versionAndInfo <- ask
      withPkgAsGamma presentPkg $
        case mbPkgVersion of
            Nothing -> do
              when (pkgSupportsUpgrades presentPkg && PackageName "daml-prim" /= packageName) $
                diagnosticWithContext $ WErrorToWarning $ WEDependencyHasNoMetadataDespiteUpgradeability presentPkgId UpgradedPackage
              pure $ Just (packageName, (Nothing, presentPkgId, presentPkg))
            Just packageVersion ->
              case splitPackageVersion id packageVersion of
                Left version -> do
                  diagnosticWithContext $ WErrorToWarning $ WEDependencyHasUnparseableVersion packageName version UpgradedPackage
                  pure Nothing
                Right presentVersion -> do
                  let result = (packageName, (Just presentVersion, presentPkgId, presentPkg))
                  unless (packageName `elem` [PackageName "daml-prim", PackageName "daml-stdlib"]) $
                    case HMS.lookup packageName upgradeablePackageMap of
                      Nothing -> pure ()
                      Just upgradeablePkgs -> do
                        let filterUpgradeablePkgs pred = mapMaybe $ \case
                              (Just v, pkgId, pkg) | pred (v, pkgId, pkg) -> Just (v, pkgId, pkg)
                              _ -> Nothing
                            equivalent = filterUpgradeablePkgs (\(pastVersion, pastPkgId, _) -> pastVersion == presentVersion && pastPkgId /= presentPkgId) upgradeablePkgs
                            ordFst = compare `on` (\(v,_,_) -> v)
                            closestGreater = minimumByMay ordFst $ filterUpgradeablePkgs (\(pastVersion, _, _) -> pastVersion > presentVersion) upgradeablePkgs
                            closestLesser = maximumByMay ordFst $ filterUpgradeablePkgs (\(pastVersion, _, _) -> pastVersion < presentVersion) upgradeablePkgs
                        if not (null equivalent)
                          then
                            throwWithContext $ EUpgradeMultiplePackagesWithSameNameAndVersion packageName presentVersion (presentPkgId : map (\(_,id,_) -> id) equivalent)
                          else do
                            let otherDepsWithSelf = upgradeablePackageMapToDeps $ addDep result upgradeablePackageMap
                            case closestGreater of
                              Just (greaterPkgVersion, _greaterPkgId, greaterPkg) -> withReaderT (const versionAndInfo) $ do
                                let context = ContextDefUpgrading { cduPkgName = packageName, cduPkgVersion = Upgrading greaterPkgVersion presentVersion, cduSubContext = ContextNone, cduIsDependency = True }
                                checkPackageBoth
                                  CheckAll
                                  (Just context)
                                  greaterPkg
                                  ((presentPkgId, presentPkg), otherDepsWithSelf)
                                checkPackageSingle
                                  (Just context)
                                  presentPkg
                              Nothing ->
                                pure ()
                            case closestLesser of
                              Just (lesserPkgVersion, lesserPkgId, lesserPkg) -> withReaderT (const versionAndInfo) $ do
                                let context = ContextDefUpgrading { cduPkgName = packageName, cduPkgVersion = Upgrading lesserPkgVersion presentVersion, cduSubContext = ContextNone, cduIsDependency = True }
                                checkPackageBoth
                                  CheckAll
                                  (Just context)
                                  presentPkg
                                  ((lesserPkgId, lesserPkg), otherDepsWithSelf)
                                checkPackageSingle
                                  (Just context)
                                  presentPkg
                              Nothing ->
                                pure ()
                  pure (Just result)

checkPackageM :: CheckDepth -> LF.UpgradedPackageId -> Upgrading LF.Package -> TcUpgradeM ()
checkPackageM checkDepth upgradedPackageId package = do
    (upgradedModules, _new) <- checkDeleted (EUpgradeMissingModule . NM.name) $ NM.toHashMap . packageModules <$> package
    case checkDepth of
      CheckAll -> forM_ upgradedModules $ checkModuleM upgradedPackageId
      CheckOnlyMissingModules -> pure ()

extractDelExistNew
    :: (Eq k, Hashable k)
    => Upgrading (HMS.HashMap k a)
    -> (HMS.HashMap k a, HMS.HashMap k (Upgrading a), HMS.HashMap k a)
extractDelExistNew Upgrading{..} =
    ( _past `HMS.difference` _present
    , HMS.intersectionWith Upgrading _past _present
    , _present `HMS.difference` _past
    )

checkDeleted
    :: (Eq k, Hashable k, SomeErrorOrWarning e)
    => (a -> e)
    -> Upgrading (HMS.HashMap k a)
    -> TcUpgradeM (HMS.HashMap k (Upgrading a), HMS.HashMap k a)
checkDeleted handleError upgrade =
    checkDeletedG ((Nothing,) . handleError) upgrade

checkDeletedWithContext
    :: (Eq k, Hashable k, SomeErrorOrWarning e)
    => (a -> (Context, e))
    -> Upgrading (HMS.HashMap k a)
    -> TcUpgradeM (HMS.HashMap k (Upgrading a), HMS.HashMap k a)
checkDeletedWithContext handleError upgrade =
    checkDeletedG (first Just . handleError) upgrade

checkDeletedG
    :: (Eq k, Hashable k, SomeErrorOrWarning e)
    => (a -> (Maybe Context, e))
    -> Upgrading (HMS.HashMap k a)
    -> TcUpgradeM (HMS.HashMap k (Upgrading a), HMS.HashMap k a)
checkDeletedG handleError upgrade = do
    let (deleted, existing, new) = extractDelExistNew upgrade
    throwIfNonEmpty handleError deleted
    pure (existing, new)

throwIfNonEmpty
    :: (Eq k, Hashable k, SomeErrorOrWarning e)
    => (a -> (Maybe Context, e))
    -> HMS.HashMap k a
    -> TcUpgradeM ()
throwIfNonEmpty handleError hm =
    case HMS.toList hm of
      ((_, first):_) ->
          let (ctx, err) = handleError first
              ctxHandler =
                  case ctx of
                    Nothing -> id
                    Just ctx -> withContextF present' ctx
          in
          ctxHandler $ diagnosticWithContextF present' err
      _ -> pure ()

checkModuleM :: LF.UpgradedPackageId -> Upgrading LF.Module -> TcUpgradeM ()
checkModuleM upgradedPackageId module_ = do
    (existingTemplates, newTemplates) <- checkDeleted (EUpgradeMissingTemplate . NM.name) $ NM.toHashMap . moduleTemplates <$> module_
    forM_ existingTemplates $ \template ->
        withContextF
            present'
            (ContextTemplate (_present module_) (_present template) TPWhole)
            (checkTemplate module_ template)

    -- For a datatype, derive its context
    let deriveChoiceInfo :: LF.Module -> HMS.HashMap LF.TypeConName (LF.Template, LF.TemplateChoice)
        deriveChoiceInfo module_ = HMS.fromList $ do
            template <- NM.toList (moduleTemplates module_)
            choice <- NM.toList (tplChoices template)
            TCon dtName <- [snd (chcArgBinder choice)] -- Choice inputs should always be type constructors
            pure (qualObject dtName, (template, choice))
        deriveVariantInfo :: LF.Module -> HMS.HashMap LF.TypeConName (LF.DefDataType, LF.VariantConName)
        deriveVariantInfo module_ = HMS.fromList $ do
            dataType <- NM.toList (moduleDataTypes module_)
            DataVariant variants <- pure $ dataCons dataType
            (variantName, TConApp recordName _) <- variants
            pure (qualObject recordName, (dataType, variantName))
        dataTypeOrigin
            :: DefDataType -> Module
            -> (UpgradedRecordOrigin, Context)
        dataTypeOrigin dt module_
            | Just template <- NM.name dt `NM.lookup` moduleTemplates module_ =
                ( TemplateBody (NM.name dt)
                , ContextTemplate module_ template TPWhole
                )
            | Just (template, choice) <- NM.name dt `HMS.lookup` deriveChoiceInfo module_ =
                ( TemplateChoiceInput (NM.name template) (NM.name choice)
                , ContextTemplate module_ template (TPChoice choice)
                )
            | Just (variant, variantName) <- NM.name dt `HMS.lookup` deriveVariantInfo module_ =
                ( VariantConstructor (dataTypeCon variant) variantName
                , ContextDefDataType module_ variant
                )
            | otherwise =
                ( TopLevel (dataTypeCon dt)
                , ContextDefDataType module_ dt
                )

    let ifaceDts :: Upgrading (HMS.HashMap LF.TypeConName (DefDataType, DefInterface))
        unownedDts :: Upgrading (HMS.HashMap LF.TypeConName DefDataType)
        (ifaceDts, unownedDts) =
            let Upgrading
                    { _past = (pastIfaceDts, pastUnownedDts)
                    , _present = (presentIfaceDts, presentUnownedDts)
                    } = fmap splitModuleDts module_
            in
            ( Upgrading pastIfaceDts presentIfaceDts
            , Upgrading pastUnownedDts presentUnownedDts
            )

        splitModuleDts
            :: Module
            -> ( HMS.HashMap LF.TypeConName (DefDataType, DefInterface)
               , HMS.HashMap LF.TypeConName DefDataType)
        splitModuleDts module_ =
            let (ifaceDtsList, unownedDtsList) =
                    partitionEithers
                        $ map (\(tcon, def) -> lookupInterface module_ tcon def)
                        $ HMS.toList $ NM.toHashMap $ moduleDataTypes module_
            in
            (HMS.fromList ifaceDtsList, HMS.fromList unownedDtsList)

        lookupInterface
            :: Module -> LF.TypeConName -> DefDataType
            -> Either (LF.TypeConName, (DefDataType, DefInterface)) (LF.TypeConName, DefDataType)
        lookupInterface module_ tcon datatype =
            case NM.name datatype `NM.lookup` moduleInterfaces module_ of
              Nothing -> Right (tcon, datatype)
              Just iface -> Left (tcon, (datatype, iface))

    -- Check that no interfaces have been deleted, nor propagated
    -- New interface checks are handled by `checkNewInterfacesHaveNoTemplates`,
    -- invoked in `singlePkgDiagnostics` above
    -- Interface deletion is the correct behaviour so we ignore that
    let (_ifaceDel, ifaceExisting, _ifaceNew) = extractDelExistNew ifaceDts
    checkContinuedIfaces module_ ifaceExisting

    let flattenInstances
            :: Module
            -> HMS.HashMap (TypeConName, Qualified TypeConName) (Template, TemplateImplements)
        flattenInstances module_ = HMS.fromList
            [ ((NM.name template, NM.name implementation), (template, implementation))
            | template <- NM.elems (moduleTemplates module_)
            , implementation <- NM.elems (tplImplements template)
            ]
    let (instanceDel, _instanceExisting, instanceNew) = extractDelExistNew (flattenInstances <$> module_)
    let notANewTemplate (tyCon, _) _ = not (HMS.member tyCon newTemplates)
    checkDeletedInstances (_present module_) instanceDel
    checkAddedInstances (_present module_) (HMS.filterWithKey notANewTemplate instanceNew)

    checkUpgradedInterfacesAreUnused upgradedPackageId (_present module_) instanceNew

    -- checkDeleted should only trigger on datatypes not belonging to templates or choices or interfaces, which we checked above
    (dtExisting, _dtNew) <- checkDeleted (EUpgradeMissingDataCon . NM.name) unownedDts

    forM_ dtExisting $ \dt ->
        -- Get origin/context for each datatype in both _past and _present
        let origin = dataTypeOrigin <$> dt <*> module_
        in
        -- If origins don't match, record has changed origin
        if foldU (/=) (fst <$> origin) then
            withContextF present' (ContextDefDataType (_present module_) (_present dt)) $
                throwWithContextF present' (EUpgradeRecordChangedOrigin (dataTypeCon (_present dt)) (fst (_past origin)) (fst (_present origin)))
        else do
            let (presentOrigin, context) = _present origin
            withContextF present' context $ checkDefDataType presentOrigin dt

checkDeletedInstances ::
    Module ->
    HMS.HashMap (TypeConName, Qualified TypeConName) (Template, TemplateImplements) ->
    TcUpgradeM ()
checkDeletedInstances module_ instances = throwIfNonEmpty handleError instances
  where
    handleError ::
        (Template, TemplateImplements) -> (Maybe Context, UnwarnableError)
    handleError (tpl, impl) =
        ( Just (ContextTemplate module_ tpl TPWhole)
        , EUpgradeMissingImplementation (NM.name tpl) (LF.qualObject (NM.name impl))
        )

checkAddedInstances ::
    Module ->
    HMS.HashMap (TypeConName, Qualified TypeConName) (Template, TemplateImplements) ->
    TcUpgradeM ()
checkAddedInstances module_ instances = throwIfNonEmpty handleError instances
  where
    handleError ::
        (Template, TemplateImplements) -> (Maybe Context, UnwarnableError)
    handleError (tpl, impl) =
        ( Just (ContextTemplate module_ tpl TPWhole)
        , EForbiddenNewImplementation (NM.name tpl) (LF.qualObject (NM.name impl))
        )

-- It is always invalid to keep an interface in an upgrade
checkContinuedIfaces
    :: Upgrading Module
    -> HMS.HashMap LF.TypeConName (Upgrading (DefDataType, DefInterface))
    -> TcUpgradeM ()
checkContinuedIfaces module_ ifaces =
    forM_ ifaces $ \upgradedDtIface ->
        let (_dt, iface) = _present upgradedDtIface
        in
        withContextF present' (ContextDefInterface (_present module_) iface IPWhole) $
            throwWithContextF present' $ EUpgradeTriedToUpgradeIface (NM.name iface)

class HasModules a where
  getModules :: a -> NM.NameMap LF.Module

instance HasModules LF.Module where
  getModules module_ = NM.singleton module_

instance HasModules LF.Package where
  getModules pkg = LF.packageModules pkg

instance HasModules [LF.Module] where
  getModules = NM.fromList

-- Check that a module or package does not define both interfaces and templates.
-- This warning should trigger even when no previous version DAR is specified in
-- the `upgrades:` field.
checkNewInterfacesHaveNoTemplates :: TcM ()
checkNewInterfacesHaveNoTemplates = do
    modules <- NM.toList . packageModules . getWorldSelf <$> getWorld
    let templateDefined = filter (not . NM.null . moduleTemplates) modules
        interfaceDefined = filter (not . NM.null . moduleInterfaces) modules
    when (not (null templateDefined) && not (null interfaceDefined)) $
        diagnosticWithContext WEUpgradeShouldDefineIfacesAndTemplatesSeparately

-- Check that any interfaces defined in this package or module do not also have
-- an instance. Interfaces defined in other packages are allowed to have
-- instances.
-- This warning should trigger even when no previous version DAR is specified in
-- the `upgrades:` field.
checkNewInterfacesAreUnused :: HasModules a => a -> TcM ()
checkNewInterfacesAreUnused hasModules =
    forM_ (HMS.toList (instantiatedIfaces modules)) $ \(ifaceQualName, modTplImpls) ->
        case qualPackage ifaceQualName of
          PRSelf -> do
            defIface <- inWorld (lookupInterface ifaceQualName)
            forM_ modTplImpls $ \modTplImpl -> do
              let (instanceModule, instanceTpl, instanceTplName, instanceImpl) = modTplImpl
              withContext (ContextTemplate instanceModule instanceTpl (TPInterfaceInstance (InterfaceInstanceHead (tpiInterface instanceImpl) instanceTplName) (tpiLocation instanceImpl))) $
                  diagnosticWithContext $ WEUpgradeShouldDefineIfaceWithoutImplementation (NM.name instanceModule) (NM.name defIface) (qualObject instanceTplName)
          _ -> pure ()
    where
    modules = getModules hasModules

-- When upgrading a package, check that the interfaces defined in the previous
-- version are not instantiated by any templates in the current version.
checkUpgradedInterfacesAreUnused
    :: LF.UpgradedPackageId
    -> Module
    -> HMS.HashMap (TypeConName, Qualified TypeConName) (Template, TemplateImplements)
    -> TcUpgradeM ()
checkUpgradedInterfacesAreUnused upgradedPackageId module_ newInstances = do
    forM_ (HMS.toList newInstances) $ \((tplName, ifaceName), (tpl, implementation)) ->
        when (fromUpgradedPackage ifaceName) $
            let qualifiedTplName = Qualified PRSelf (moduleName module_) tplName
                ifaceInstanceHead = InterfaceInstanceHead ifaceName qualifiedTplName
            in
            withContextF present' (ContextTemplate module_ tpl (TPInterfaceInstance ifaceInstanceHead Nothing)) $
                diagnosticWithContextF present' $ WEUpgradeShouldDefineTplInSeparatePackage (NM.name tpl) (LF.qualObject (NM.name implementation))
    where
    fromUpgradedPackage :: forall a. LF.Qualified a -> Bool
    fromUpgradedPackage identifier =
        case qualPackage identifier of
          PRImport identifierOrigin -> unUpgradedPackageId upgradedPackageId == identifierOrigin
          _ -> False

instantiatedIfaces :: NM.NameMap LF.Module -> HMS.HashMap (LF.Qualified LF.TypeConName) [(Module, Template, Qualified TypeConName, TemplateImplements)]
instantiatedIfaces modules = foldl' (HMS.unionWith (<>)) HMS.empty $ (map . fmap) pure
    [ HMS.map (module_,template,qualifiedTemplateName,) $ NM.toHashMap $ tplImplements template
    | module_ <- NM.elems modules
    , template <- NM.elems (moduleTemplates module_)
    , let qualifiedTemplateName = Qualified PRSelf (NM.name module_) (NM.name template)
    ]

checkTemplate :: Upgrading Module -> Upgrading LF.Template -> TcUpgradeM ()
checkTemplate module_ template = do
    -- Check that no choices have been removed
    (existingChoices, _existingNew) <- checkDeleted (EUpgradeMissingChoice . NM.name) $ NM.toHashMap . tplChoices <$> template
    forM_ existingChoices $ \choice -> do
        withContextF present' (ContextTemplate (_present module_) (_present template) (TPChoice (_present choice))) $ do
            unlessM (isUpgradedTypeNoTypeVars (fmap chcReturnType choice)) $
                throwWithContextF present' (EUpgradeChoiceChangedReturnType (NM.name (_present choice)))

            whenDifferent "controllers" (extractFuncFromFuncThisArg . chcControllers) choice $
                \mismatches -> warnWithContextF present' (WChoiceChangedControllers (NM.name (_present choice)) mismatches)

            let observersErr = WChoiceChangedObservers $ NM.name $ _present choice
            case fmap (mapENilToNothing . chcObservers) choice of
               Upgrading { _past = Nothing, _present = Nothing } -> do
                   pure ()
               Upgrading { _past = Just _past, _present = Just _present } -> do
                   whenDifferent "observers"
                       extractFuncFromFuncThisArg (Upgrading _past _present)
                       (\mismatches -> warnWithContextF present' (observersErr mismatches))
               _ -> do
                   warnWithContextF present' (observersErr [])

            let authorizersErr = WChoiceChangedAuthorizers $ NM.name $ _present choice
            case fmap (mapENilToNothing . chcAuthorizers) choice of
               Upgrading { _past = Nothing, _present = Nothing } -> pure ()
               Upgrading { _past = Just _past, _present = Just _present } ->
                   whenDifferent "authorizers"
                       extractFuncFromFuncThisArg (Upgrading _past _present)
                       (\mismatches -> warnWithContextF present' (authorizersErr mismatches))
               _ -> warnWithContextF present' (authorizersErr [])
        pure choice

    -- This check assumes that we encode signatories etc. on a template as
    -- $<uniquename> this, where $<uniquename> is a function that contains the
    -- actual definition. We resolve this function and check that it is
    -- identical.
    withContextF present' (ContextTemplate (_present module_) (_present template) TPPrecondition) $
        whenDifferent "precondition" (extractFuncFromCaseFuncThis . tplPrecondition) template $
            \mismatches -> warnWithContextF present' $ WTemplateChangedPrecondition (NM.name (_present template)) mismatches
    withContextF present' (ContextTemplate (_present module_) (_present template) TPSignatories) $
        whenDifferent "signatories" (extractFuncFromFuncThis . tplSignatories) template $
            \mismatches -> warnWithContextF present' $ WTemplateChangedSignatories (NM.name (_present template)) mismatches
    withContextF present' (ContextTemplate (_present module_) (_present template) TPObservers) $
        whenDifferent "observers" (extractFuncFromFuncThis . tplObservers) template $
            \mismatches -> warnWithContextF present' $ WTemplateChangedObservers (NM.name (_present template)) mismatches
    withContextF present' (ContextTemplate (_present module_) (_present template) TPAgreement) $
        whenDifferent "agreement" (extractFuncFromFuncThis . tplAgreement) template $
            \mismatches -> warnWithContextF present' $ WTemplateChangedAgreement (NM.name (_present template)) mismatches

    withContextF present' (ContextTemplate (_present module_) (_present template) TPKey) $ do
        case fmap tplKey template of
           Upgrading { _past = Nothing, _present = Nothing } -> do
               pure ()
           Upgrading { _past = Just pastKey, _present = Just presentKey } -> do
               let tplKey = Upgrading pastKey presentKey

               -- Key type must be a valid upgrade
               iset <- isUpgradedTypeNoTypeVars (fmap tplKeyType tplKey)
               when (not iset) $
                   diagnosticWithContextF present' (EUpgradeTemplateChangedKeyType (NM.name (_present template)))

               -- But expression for computing it may
               whenDifferent "key expression"
                   (extractFuncFromFuncThis . tplKeyBody) tplKey
                   (\mismatches -> warnWithContextF present' $ WTemplateChangedKeyExpression (NM.name (_present template)) mismatches)
               whenDifferent "key maintainers"
                   (extractFuncFromTyAppNil . tplKeyMaintainers) tplKey
                   (\mismatches -> warnWithContextF present' $ WTemplateChangedKeyMaintainers (NM.name (_present template)) mismatches)
           Upgrading { _past = Just pastKey, _present = Nothing } ->
               throwWithContextF present' $ EUpgradeTemplateRemovedKey (NM.name (_present template)) pastKey
           Upgrading { _past = Nothing, _present = Just presentKey } ->
               throwWithContextF present' $ EUpgradeTemplateAddedKey (NM.name (_present template)) presentKey

    -- TODO: Check that return type of a choice is compatible
    pure ()
    where
        mapENilToNothing :: Maybe Expr -> Maybe Expr
        mapENilToNothing (Just (LF.ENil (LF.TBuiltin LF.BTParty))) = Nothing
        mapENilToNothing e = e

        -- Given an extractor from the list below, whenDifferent runs an action
        -- when the relevant expressions differ.
        whenDifferent :: Show a => String -> (a -> Module -> Either String Expr) -> Upgrading a -> ([Alpha.Mismatch UpgradeMismatchReason] -> TcUpgradeM ()) -> TcUpgradeM ()
        whenDifferent field extractor exprs act =
            let resolvedWithPossibleError = sequence $ extractor <$> exprs <*> module_
            in
            case resolvedWithPossibleError of
                Left err ->
                    warnWithContextF present' (WCouldNotExtractForUpgradeChecking (T.pack field) (Just (T.pack err)))
                Right resolvedExprs -> do
                    alphaEnv <- upgradingAlphaEnv
                    let exprsMatchWarnings = foldU (Alpha.alphaExpr' alphaEnv) $ fmap removeLocations resolvedExprs
                    unless (null exprsMatchWarnings) (act exprsMatchWarnings)

        -- Each extract function takes an expression, extracts a relevant
        -- ExprValName, and performs lookups necessary to get the actual
        -- definition.

        -- Given an expression in a module:
        -- $mydef this
        -- Extract the definition of $mydef from the module
        extractFuncFromFuncThis :: Expr -> Module -> Either String Expr
        extractFuncFromFuncThis expr module_
            | ETmApp{..} <- expr
            , EVal qualEvn <- tmappFun
            , EVar (ExprVarName "this") <- tmappArg
            = lookupInModule module_ (qualObject qualEvn)
            | otherwise
            = Left "extractFuncFromFuncThis: Wrong shape"

        -- Given an expression in a module:
        -- $mydef this arg
        -- Extract the definition of $mydef from the module
        extractFuncFromFuncThisArg :: Expr -> Module -> Either String Expr
        extractFuncFromFuncThisArg expr module_
            | outer@ETmApp{} <- expr
            , EVar (ExprVarName "arg") <- tmappArg outer
            , inner@ETmApp{} <- tmappFun outer
            , EVar (ExprVarName "this") <- tmappArg inner
            , EVal qualEvn <- tmappFun inner
            = lookupInModule module_ (qualObject qualEvn)
            | otherwise
            = Left "extractFuncFromFuncThisArg: Wrong shape"

        -- Given an expression in a module:
        -- case $mydef this of ...
        -- Extract the definition of $mydef from the module
        extractFuncFromCaseFuncThis :: Expr -> Module -> Either String Expr
        extractFuncFromCaseFuncThis expr module_
            | ECase{..} <- expr
            = extractFuncFromFuncThis casScrutinee module_
            | otherwise
            = Left "extractFuncFromCaseFuncThis: No ECase found"

        -- Given an expression in a module:
        -- $mydef @[] []
        -- where $mydef is a term of the shape for extractFuncFromProxyApp
        -- Extract the internals of $mydef using extractFuncFromProxyApp
        extractFuncFromTyAppNil :: Expr -> Module -> Either String Expr
        extractFuncFromTyAppNil expr module_
            | outer@ETmApp{} <- expr
            , ENil{} <- tmappArg outer
            , inner@ETyApp{} <- tmappFun outer
            , TBuiltin BTList <- tyappType inner
            , EVal qualEvn <- tyappExpr inner
            = do
                definition <- lookupInModule module_ (qualObject qualEvn)
                extractFuncFromProxyApp definition module_
            | otherwise
            = Left "extractFuncFromTyAppNil: Wrong shape"

        -- Given an expression in a module:
        -- ∀(proxy : * -> *). \(_arg : proxy a) -> $mydef
        -- Extract the definition of $mydef from the module
        extractFuncFromProxyApp :: Expr -> Module -> Either String Expr
        extractFuncFromProxyApp expr module_
            | outer@ETyLam{} <- expr
            , (TypeVarName "proxy", KArrow KStar KStar) <- tylamBinder outer
            , inner@ETmLam{} <- tylamBody outer
            , (_, TApp (TVar (TypeVarName "proxy")) _) <- tmlamBinder inner
            , EVal qualEvn <- tmlamBody inner
            = lookupInModule module_ (qualObject qualEvn)
            | otherwise
            = Left "extractFuncFromProxyApp: Wrong shape"

        lookupInModule :: Module -> ExprValName -> Either String Expr
        lookupInModule module_ evn =
            case NM.lookup evn (moduleValues module_) of
                Nothing -> Left ("checkTemplate: Trying to get definition of " ++ T.unpack (unExprValName evn) ++ " but it is not defined!")
                Just defValue -> Right (dvalBody defValue)

checkDefDataType :: UpgradedRecordOrigin -> Upgrading LF.DefDataType -> TcUpgradeM ()
checkDefDataType origin datatype = do
    let params = dataParams <$> datatype
        paramsLengthMatch = foldU (==) (length <$> params)
        allKindsMatch = foldU (==) (map snd <$> params)
    when (not paramsLengthMatch) $ throwWithContextF present' $ EUpgradeDifferentParamsCount origin
    when (not allKindsMatch) $ throwWithContextF present' $ EUpgradeDifferentParamsKinds origin
    let paramNames = unsafeZipUpgrading (map fst <$> params)
    case fmap dataCons datatype of
      Upgrading { _past = DataRecord _past, _present = DataRecord _present } ->
          checkFields origin paramNames (Upgrading {..})
      Upgrading { _past = DataVariant _past, _present = DataVariant _present } -> do
          let upgrade = Upgrading{..}
          (existing, _new) <- checkDeleted (\_ -> EUpgradeVariantRemovedVariant origin) (fmap HMS.fromList upgrade)
          when (not $ and $ foldU (zipWith (==)) $ fmap (map fst) upgrade) $
              throwWithContextF present' (EUpgradeVariantVariantsOrderChanged origin)
          different <- filterHashMapM (fmap not . isUpgradedType paramNames) existing
          when (not (null different)) $
              throwWithContextF present' $ EUpgradeVariantChangedVariantType origin
      Upgrading { _past = DataEnum _past, _present = DataEnum _present } -> do
          let upgrade = Upgrading{..}
          (_, _new) <-
              checkDeleted
                (\_ -> EUpgradeEnumRemovedVariant origin)
                (fmap (HMS.fromList . map (,())) upgrade)
          when (not $ and $ foldU (zipWith (==)) upgrade) $
              throwWithContextF present' (EUpgradeEnumVariantsOrderChanged origin)
      Upgrading { _past = DataInterface {}, _present = DataInterface {} } ->
          pure ()
      _ ->
          throwWithContextF present' (EUpgradeMismatchDataConsVariety (dataTypeCon (_past datatype)) (dataCons (_past datatype)) (dataCons (_present datatype)))

filterHashMapM :: (Applicative m) => (a -> m Bool) -> HMS.HashMap k a -> m (HMS.HashMap k a)
filterHashMapM pred t =
    fmap fst . HMS.filter snd <$> traverse (\x -> (x,) <$> pred x) t

checkFields :: UpgradedRecordOrigin -> [Upgrading TypeVarName] -> Upgrading [(FieldName, Type)] -> TcUpgradeM ()
checkFields origin paramNames fields = do
    (existing, new) <- checkDeleted (\_ -> EUpgradeRecordFieldsMissing origin) (fmap HMS.fromList fields)
    -- If a field from the upgraded package has had its type changed
    different <- filterHashMapM (fmap not . isUpgradedType paramNames) existing
    when (not (HMS.null different)) $
        throwWithContextF present' (EUpgradeRecordFieldsExistingChanged origin)
    when (not (all newFieldOptionalType new)) $
        case origin of
          VariantConstructor{} ->
            throwWithContextF present' (EUpgradeVariantAddedVariantField origin)
          _ ->
            throwWithContextF present' (EUpgradeRecordFieldsNewNonOptional origin)
        -- If a new field has a non-optional type
    -- If the order of fields changed
    when (not $ and $ foldU (zipWith (==)) $ fmap (map fst) fields) $
        throwWithContextF present' (EUpgradeRecordFieldsOrderChanged origin)
    where
        newFieldOptionalType (TOptional _) = True
        newFieldOptionalType _ = False

checkQualName :: Alpha.IsSomeName a => DepsMap -> Upgrading (Qualified a) -> [Alpha.Mismatch UpgradeMismatchReason]
checkQualName deps name =
    let namesAreSame = foldU Alpha.alphaEq' (fmap removePkgId name)
        qualificationIsSameOrUpgraded =
          case qualPackage <$> name of
            Upgrading { _past = PRImport pastPkgId, _present = PRImport presentPkgId } ->
              if pastPkgId == presentPkgId
              then []
              else pkgIdsAreUpgraded deps Upgrading { _past = pastPkgId, _present = presentPkgId }
            Upgrading PRSelf (PRImport pkgId) -> [nameMismatch' (OriginChangedFromSelfToImport (tryGetPkgId pkgId))]
            Upgrading (PRImport pkgId) PRSelf -> [nameMismatch' (OriginChangedFromImportToSelf (tryGetPkgId pkgId))]
            Upgrading PRSelf PRSelf -> []
        prioritizeMismatches mismatches
          | Alpha.StructuralMismatch `elem` mismatches = [Alpha.StructuralMismatch]
          | otherwise = mismatches
    in
    prioritizeMismatches (namesAreSame `Alpha.andMismatches` qualificationIsSameOrUpgraded)
  where
    nameMismatch' reason = foldU Alpha.nameMismatch name (Just reason)
    ifMismatch b reason = [nameMismatch' reason | not b]
    tryGetPkgId :: LF.PackageId -> Either PackageId UpgradingDep
    tryGetPkgId pkgId =
      case HMS.lookup pkgId deps of
        Nothing -> Left pkgId
        Just dep -> Right dep

    pkgIdsAreUpgraded :: DepsMap -> Upgrading PackageId -> [Alpha.Mismatch UpgradeMismatchReason]
    pkgIdsAreUpgraded deps pkgId =
      case flip HMS.lookup deps <$> pkgId of
        Upgrading { _past = Just pastDep, _present = Just presentDep } ->
          let upgradingDep = Upgrading { _past = pastDep, _present = presentDep }
          in
          ifMismatch (foldU (==) (fmap udPkgName upgradingDep)) (PackageNameChanged upgradingDep)
          `Alpha.andMismatches`
          case udVersionSupportsUpgrades <$> upgradingDep of
            Upgrading False False -> ifMismatch (foldU (==) pkgId) (DifferentPackagesNeitherOfWhichSupportsUpgrades upgradingDep)
            Upgrading True True ->
              case udIsUtilityPackage <$> upgradingDep of
                Upgrading True True ->
                  if udPkgName (_present upgradingDep) `elem` [PackageName "daml-stdlib", PackageName "daml-prim"]
                  then []
                  else case udMbPackageVersion `traverse` upgradingDep of
                    Just version -> ifMismatch (_past version > _present version) (PastPackageHasHigherVersion upgradingDep)
                    _ -> [] -- This case should not be possible
                Upgrading False False ->
                  case udMbPackageVersion <$> upgradingDep of
                      Upgrading (Just pastVersion) (Just presentVersion) -> ifMismatch (pastVersion < presentVersion) (PastPackageHasHigherVersion upgradingDep)
                      Upgrading Nothing Nothing -> [] -- Only daml-prim has no version field
                      _ -> [] -- This case should not be possible
                Upgrading False True -> [nameMismatch' (PackageChangedFromSchemaToUtilityPackage upgradingDep)]
                Upgrading True False -> [nameMismatch' (PackageChangedFromUtilityToSchemaPackage upgradingDep)]
            Upgrading False True -> [nameMismatch' (PackageChangedFromDoesNotSupportUpgradesToSupportUpgrades upgradingDep)]
            Upgrading True False -> [nameMismatch' (PackageChangedFromSupportUpgradesToDoesNotSupportUpgrades upgradingDep)]
        Upgrading { _past = pastDepLookup, _present = presentDepLookup } ->
          ifMismatch (isNothing pastDepLookup) (CouldNotFindPackageForPastIdentifier (Left (_past pkgId)))
          `Alpha.andMismatches` ifMismatch (isNothing presentDepLookup) (CouldNotFindPackageForPastIdentifier (Left (_present pkgId)))

isUpgradedTypeNoTypeVars :: Upgrading Type -> TcUpgradeM Bool
isUpgradedTypeNoTypeVars = isUpgradedType []

isUpgradedType :: [Upgrading TypeVarName] -> Upgrading Type -> TcUpgradeM Bool
isUpgradedType varNames type_ = do
    expandedTypes <- runGammaUnderUpgrades (expandTypeSynonyms <$> type_)
    alphaEnv <- upgradingAlphaEnv
    let alphaEnvWithVars = foldl' (flip (foldU Alpha.bindTypeVar)) alphaEnv varNames
    -- NOTE: The warning messages generated by alphaType' via checkQualName
    -- below are only designed for the expression warnings and won't make sense
    -- if used to describe issues with types.
    pure $ null $ foldU (Alpha.alphaType' alphaEnvWithVars) expandedTypes

upgradingAlphaEnv :: TcUpgradeM (Alpha.AlphaEnv UpgradeMismatchReason)
upgradingAlphaEnv = do
    depsMap <- view depsMap
    pure $ Alpha.initialAlphaEnv { Alpha.alphaTypeCon = unfoldU (checkQualName depsMap), Alpha.alphaExprVal = unfoldU (checkQualName depsMap) }

removePkgId :: Qualified a -> Qualified a
removePkgId a = a { qualPackage = PRSelf }
