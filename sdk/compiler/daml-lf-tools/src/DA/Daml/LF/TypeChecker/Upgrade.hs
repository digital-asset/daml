-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE TemplateHaskell #-}

module DA.Daml.LF.TypeChecker.Upgrade (
        module DA.Daml.LF.TypeChecker.Upgrade
    ) where

import           Control.DeepSeq
import           Control.Monad (unless, forM, forM_, when)
import           Control.Monad.Extra (allM, filterM)
import           Control.Monad.Reader (withReaderT, local)
import           Control.Lens hiding (Context)
import           DA.Daml.LF.Ast as LF
import           DA.Daml.LF.Ast.Alpha (alphaExpr, AlphaEnv(..), initialAlphaEnv, alphaType', alphaTypeCon, bindTypeVar)
import           DA.Daml.LF.TypeChecker.Check (expandTypeSynonyms)
import           DA.Daml.LF.TypeChecker.Env
import           DA.Daml.LF.TypeChecker.Error
import           DA.Daml.Options.Types (UpgradeInfo (..))
import           Data.Bifunctor (first)
import           Data.Data
import           Data.Either (partitionEithers)
import           Data.Hashable
import qualified Data.HashMap.Strict as HMS
import           Data.List (foldl')
import qualified Data.NameMap as NM
import qualified Data.Text as T
import           Development.IDE.Types.Diagnostics
import           GHC.Generics (Generic)

data Upgrading a = Upgrading
    { _past :: a
    , _present :: a
    }
    deriving (Eq, Data, Generic, NFData, Show)

makeLenses ''Upgrading

instance Functor Upgrading where
    fmap f Upgrading{..} = Upgrading (f _past) (f _present)

instance Foldable Upgrading where
    foldMap f Upgrading{..} = f _past <> f _present

instance Traversable Upgrading where
    traverse f Upgrading{..} = Upgrading <$> f _past <*> f _present

instance Applicative Upgrading where
    pure a = Upgrading a a
    (<*>) f a = Upgrading { _past = _past f (_past a), _present = _present f (_present a) }

foldU :: (a -> a -> b) -> Upgrading a -> b
foldU f u = f (_past u) (_present u)

unsafeFlattenUpgrading :: Upgrading [a] -> [Upgrading a]
unsafeFlattenUpgrading = foldU (zipWith Upgrading)

-- Allows us to split the world into upgraded and non-upgraded
type TcUpgradeM = TcMF UpgradingEnv

data UpgradingEnv = UpgradingEnv
  { _upgradingGamma :: Upgrading Gamma
  , _upgradingDeps :: [Upgrading LF.PackageId]
  }

makeLenses ''UpgradingEnv

present' :: Functor f => (Gamma -> f Gamma) -> UpgradingEnv -> f UpgradingEnv
present' = upgradingGamma . present

runGammaUnderUpgrades :: Upgrading (TcM a) -> TcUpgradeM (Upgrading a)
runGammaUnderUpgrades Upgrading{ _past = pastAction, _present = presentAction } = do
    pastResult <- withReaderT (_past . _upgradingGamma) pastAction
    presentResult <- withReaderT (_present . _upgradingGamma) presentAction
    pure Upgrading { _past = pastResult, _present = presentResult }

checkBothAndSingle
  :: World -> (LF.UpgradedPackageId -> LF.Package -> [(LF.PackageId, LF.Package)] -> TcUpgradeM ()) -> TcM ()
  -> Version -> UpgradeInfo
  -> Maybe ((LF.PackageId, LF.Package), [(LF.PackageId, LF.Package)])
  -> [Diagnostic]
checkBothAndSingle world checkBoth checkSingle version upgradeInfo mbUpgradedPackage =
    let gamma :: World -> Gamma
        gamma world =
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

        bothPkgDiagnostics :: Either Error ((), [Warning])
        bothPkgDiagnostics =
            case mbUpgradedPackage of
                Nothing ->
                    Right ((), [])
                Just ((pastPkgId, pastPkg), deps) ->
                    let upgradingWorld = Upgrading { _past = initWorldSelf [] pastPkg, _present = world }
                        upgradingEnv =
                          UpgradingEnv
                            { _upgradingGamma = fmap gamma upgradingWorld
                            , _upgradingDeps = []
                            }
                    in
                    runGammaF upgradingEnv $
                        when (uiTypecheckUpgrades upgradeInfo) (checkBoth (UpgradedPackageId pastPkgId) pastPkg deps)

        singlePkgDiagnostics :: Either Error ((), [Warning])
        singlePkgDiagnostics =
            runGammaF (gamma world) $
                when (uiTypecheckUpgrades upgradeInfo) checkSingle

        extractDiagnostics :: Either Error ((), [Warning]) -> [Diagnostic]
        extractDiagnostics result =
            case result of
              Left err -> [toDiagnostic err]
              Right ((), warnings) -> map toDiagnostic warnings
    in
    extractDiagnostics bothPkgDiagnostics ++ extractDiagnostics singlePkgDiagnostics

checkUpgrade
  :: LF.Package -> [LF.DalfPackage]
  -> Version -> UpgradeInfo
  -> Maybe ((LF.PackageId, LF.Package), [(LF.PackageId, LF.Package)])
  -> [Diagnostic]
checkUpgrade pkg deps =
    let world = initWorldSelf [] pkg
        checkBoth upgradedPkgId upgradedPkg upgradedDeps = do
          deps <- checkUpgradeDependenciesM deps upgradedDeps
          local (\env -> env { _upgradingDeps = deps }) $
            checkUpgradeM upgradedPkgId (Upgrading upgradedPkg pkg)
        checkSingle = do
          checkNewInterfacesAreUnused pkg
          checkNewInterfacesHaveNoTemplates pkg
    in
    checkBothAndSingle world checkBoth checkSingle

checkModule
  :: LF.World -> LF.Module -> [LF.DalfPackage]
  -> Version -> UpgradeInfo
  -> Maybe ((LF.PackageId, LF.Package), [(LF.PackageId, LF.Package)])
  -> [Diagnostic]
checkModule world0 module_ deps =
    let world = extendWorldSelf module_ world0
        checkBoth upgradedPkgId upgradedPkg upgradedDeps = do
          deps <- checkUpgradeDependenciesM deps upgradedDeps
          case NM.lookup (NM.name module_) (LF.packageModules upgradedPkg) of
            Nothing -> pure ()
            Just pastModule -> do
              let upgradingModule = Upgrading { _past = pastModule, _present = module_ }
              equalDataTypes <- structurallyEqualDataTypes upgradingModule
              local (\env -> env { _upgradingDeps = deps }) $
                checkModuleM equalDataTypes upgradedPkgId upgradingModule
        checkSingle = do
          checkNewInterfacesAreUnused module_
          checkNewInterfacesHaveNoTemplates module_
    in
    checkBothAndSingle world checkBoth checkSingle

checkUpgradeDependenciesM
    :: [LF.DalfPackage]
    -> [(LF.PackageId, LF.Package)]
    -> TcUpgradeM [Upgrading LF.PackageId]
checkUpgradeDependenciesM presentDeps pastDeps = do
    let pkgToTriple :: LF.PackageId -> LF.Package -> (LF.PackageName, (LF.PackageVersion, LF.PackageId))
        pkgToTriple packageId LF.Package{packageMetadata = LF.PackageMetadata{packageName, packageVersion}} = (packageName, (packageVersion, packageId))
        dalfPkgToTriple LF.DalfPackage{dalfPackageId,dalfPackagePkg} =
          pkgToTriple dalfPackageId (extPackagePkg dalfPackagePkg)
    let upgrading = Upgrading
          { _past = HMS.fromList $ map (uncurry pkgToTriple) pastDeps
          , _present = HMS.fromList $ map dalfPkgToTriple presentDeps
          }
    let (_del, existingDeps, _new) = extractDelExistNew upgrading
    depRelations <- forM (HMS.toList existingDeps) $ \(depName, dep) ->
        let versions = fmap fst dep
            pkgIds = fmap snd dep
        in
        case foldU LF.comparePackageVersion versions of
          Left (FirstVersionUnparseable presentVersion) -> do
            diagnosticWithContextF present' $
              WEPresentDependencyHasUnparseableVersion depName presentVersion
            pure []
          Left (SecondVersionUnparseable pastVersion) -> do
            diagnosticWithContextF present' $
              WEPastDependencyHasUnparseableVersion depName pastVersion
            pure []
          Right GT ->
            throwWithContextF present' $
              EUpgradeDependencyHasLowerVersionDespiteUpgrade depName (_present versions) (_past versions)
          _ -> pure [pkgIds] -- if past package is lesser than or equal, the dependency is a valid upgrade
    pure (concat depRelations)

checkUpgradeM :: LF.UpgradedPackageId -> Upgrading LF.Package -> TcUpgradeM ()
checkUpgradeM upgradedPackageId package = do
    (upgradedModules, _new) <- checkDeleted (EUpgradeMissingModule . NM.name) $ NM.toHashMap . packageModules <$> package
    equalDataTypes <- structurallyEqualDataTypes package
    forM_ upgradedModules $ checkModuleM equalDataTypes upgradedPackageId

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

checkModuleM :: [LF.TypeConName] -> LF.UpgradedPackageId -> Upgrading LF.Module -> TcUpgradeM ()
checkModuleM equalDataTypes upgradedPackageId module_ = do
    (existingTemplates, _new) <- checkDeleted (EUpgradeMissingTemplate . NM.name) $ NM.toHashMap . moduleTemplates <$> module_
    forM_ existingTemplates $ \template ->
        withContextF
            present'
            (ContextTemplate (_present module_) (_present template) TPWhole)
            (checkTemplate equalDataTypes module_ template)

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
    (_instanceExisting, instanceNew) <-
        checkDeletedWithContext
            (\(tpl, impl) ->
                ( ContextTemplate (_present module_) tpl TPWhole
                , EUpgradeMissingImplementation (NM.name tpl) (LF.qualObject (NM.name impl))
                ))
            (flattenInstances <$> module_)
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

-- Check that a module or package does not define both interfaces and templates.
-- This warning should trigger even when no previous version DAR is specified in
-- the `upgrades:` field.
checkNewInterfacesHaveNoTemplates :: HasModules a => a -> TcM ()
checkNewInterfacesHaveNoTemplates hasModules =
    let modules = NM.toHashMap (getModules hasModules)
        templateDefined = HMS.filter (not . NM.null . moduleTemplates) modules
        interfaceDefined = HMS.filter (not . NM.null . moduleInterfaces) modules
        templateAndInterfaceDefined =
            HMS.intersectionWith (,) templateDefined interfaceDefined
    in
    forM_ (HMS.toList templateAndInterfaceDefined) $ \(_, (module_, _)) ->
        withContext (ContextDefModule module_) $
            diagnosticWithContext WEUpgradeShouldDefineIfacesAndTemplatesSeparately

-- Check that any interfaces defined in this package or module do not also have
-- an instance. Interfaces defined in other packages are allowed to have
-- instances.
-- This warning should trigger even when no previous version DAR is specified in
-- the `upgrades:` field.
checkNewInterfacesAreUnused :: HasModules a => a -> TcM ()
checkNewInterfacesAreUnused hasModules =
    forM_ definedAndInstantiated $ \((module_, iface), implementations) ->
        withContext (ContextDefInterface module_ iface IPWhole) $
            diagnosticWithContext $ WEUpgradeShouldDefineIfaceWithoutImplementation (NM.name iface) ((\(_,a,_) -> NM.name a) <$> implementations)
    where
    modules = getModules hasModules

    definedIfaces :: HMS.HashMap (LF.Qualified LF.TypeConName) (Module, DefInterface)
    definedIfaces = HMS.unions
        [ HMS.mapKeys qualify $ HMS.map (module_,) $ NM.toHashMap (moduleInterfaces module_)
        | module_ <- NM.elems (getModules hasModules)
        , let qualify :: LF.TypeConName -> LF.Qualified LF.TypeConName
              qualify tcn = Qualified PRSelf (NM.name module_) tcn
        ]

    definedAndInstantiated
        :: HMS.HashMap
            (LF.Qualified LF.TypeConName)
            ((Module, DefInterface), [(Module, Template, TemplateImplements)])
    definedAndInstantiated = HMS.intersectionWith (,) definedIfaces (instantiatedIfaces modules)

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

instantiatedIfaces :: NM.NameMap LF.Module -> HMS.HashMap (LF.Qualified LF.TypeConName) [(Module, Template, TemplateImplements)]
instantiatedIfaces modules = foldl' (HMS.unionWith (<>)) HMS.empty $ (map . fmap) pure
    [ HMS.map (module_,template,) $ NM.toHashMap $ tplImplements template
    | module_ <- NM.elems modules
    , template <- NM.elems (moduleTemplates module_)
    ]

checkTemplate :: [LF.TypeConName] -> Upgrading Module -> Upgrading LF.Template -> TcUpgradeM ()
checkTemplate equalDataTypes module_ template = do
    -- Check that no choices have been removed
    (existingChoices, _existingNew) <- checkDeleted (EUpgradeMissingChoice . NM.name) $ NM.toHashMap . tplChoices <$> template
    forM_ existingChoices $ \choice -> do
        withContextF present' (ContextTemplate (_present module_) (_present template) (TPChoice (_present choice))) $ do
            checkType (fmap chcReturnType choice)
                (EUpgradeChoiceChangedReturnType (NM.name (_present choice)))

            whenDifferent "controllers" (extractFuncFromFuncThisArg . chcControllers) choice $
                warnWithContextF present' $ WChoiceChangedControllers $ NM.name $ _present choice

            let observersErr = WChoiceChangedObservers $ NM.name $ _present choice
            case fmap (mapENilToNothing . chcObservers) choice of
               Upgrading { _past = Nothing, _present = Nothing } -> do
                   pure ()
               Upgrading { _past = Just _past, _present = Just _present } -> do
                   whenDifferent "observers"
                       extractFuncFromFuncThisArg (Upgrading _past _present)
                       (warnWithContextF present' observersErr)
               _ -> do
                   warnWithContextF present' observersErr

            let authorizersErr = WChoiceChangedAuthorizers $ NM.name $ _present choice
            case fmap (mapENilToNothing . chcAuthorizers) choice of
               Upgrading { _past = Nothing, _present = Nothing } -> pure ()
               Upgrading { _past = Just _past, _present = Just _present } ->
                   whenDifferent "authorizers"
                       extractFuncFromFuncThisArg (Upgrading _past _present)
                       (warnWithContextF present' authorizersErr)
               _ -> warnWithContextF present' authorizersErr
        pure choice

    -- This check assumes that we encode signatories etc. on a template as
    -- $<uniquename> this, where $<uniquename> is a function that contains the
    -- actual definition. We resolve this function and check that it is
    -- identical.
    withContextF present' (ContextTemplate (_present module_) (_present template) TPPrecondition) $
        whenDifferent "precondition" (extractFuncFromCaseFuncThis . tplPrecondition) template $
            warnWithContextF present' $ WTemplateChangedPrecondition $ NM.name $ _present template
    withContextF present' (ContextTemplate (_present module_) (_present template) TPSignatories) $
        whenDifferent "signatories" (extractFuncFromFuncThis . tplSignatories) template $
            warnWithContextF present' $ WTemplateChangedSignatories $ NM.name $ _present template
    withContextF present' (ContextTemplate (_present module_) (_present template) TPObservers) $
        whenDifferent "observers" (extractFuncFromFuncThis . tplObservers) template $
            warnWithContextF present' $ WTemplateChangedObservers $ NM.name $ _present template

    withContextF present' (ContextTemplate (_present module_) (_present template) TPKey) $ do
        case fmap tplKey template of
           Upgrading { _past = Nothing, _present = Nothing } -> do
               pure ()
           Upgrading { _past = Just pastKey, _present = Just presentKey } -> do
               let tplKey = Upgrading pastKey presentKey

               -- Key type musn't change
               iset <- isStructurallyEquivalentType initialAlphaEnv equalDataTypes (fmap tplKeyType tplKey)
               when (not iset) $
                   diagnosticWithContextF present' (EUpgradeTemplateChangedKeyType (NM.name (_present template)))

               -- But expression for computing it may
               whenDifferent "key expression"
                   (extractFuncFromFuncThis . tplKeyBody) tplKey
                   (warnWithContextF present' $ WTemplateChangedKeyExpression $ NM.name $ _present template)
               whenDifferent "key maintainers"
                   (extractFuncFromTyAppNil . tplKeyMaintainers) tplKey
                   (warnWithContextF present' $ WTemplateChangedKeyMaintainers $ NM.name $ _present template)
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
        whenDifferent :: Show a => String -> (a -> Module -> Either String Expr) -> Upgrading a -> TcUpgradeM () -> TcUpgradeM ()
        whenDifferent field extractor exprs act =
            let resolvedWithPossibleError = sequence $ extractor <$> exprs <*> module_
            in
            case resolvedWithPossibleError of
                Left err ->
                    warnWithContextF present' (WCouldNotExtractForUpgradeChecking (T.pack field) (Just (T.pack err)))
                Right resolvedExprs ->
                    let exprsMatch = foldU alphaExpr $ fmap removeLocations resolvedExprs
                    in
                    unless exprsMatch act

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
    case fmap dataCons datatype of
      Upgrading { _past = DataRecord _past, _present = DataRecord _present } ->
          checkFields origin (Upgrading {..})
      Upgrading { _past = DataVariant _past, _present = DataVariant _present } -> do
          let upgrade = Upgrading{..}
          (existing, _new) <- checkDeleted (\_ -> EUpgradeVariantRemovedVariant origin) (fmap HMS.fromList upgrade)
          when (not $ and $ foldU (zipWith (==)) $ fmap (map fst) upgrade) $
              throwWithContextF present' (EUpgradeVariantVariantsOrderChanged origin)
          different <- filterHashMapM (fmap not . isUpgradedType) existing
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

checkFields :: UpgradedRecordOrigin -> Upgrading [(FieldName, Type)] -> TcUpgradeM ()
checkFields origin fields = do
    (existing, new) <- checkDeleted (\_ -> EUpgradeRecordFieldsMissing origin) (fmap HMS.fromList fields)
    -- If a field from the upgraded package has had its type changed
    different <- filterHashMapM (fmap not . isUpgradedType) existing
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

-- Check type upgradability
checkType :: SomeErrorOrWarning e => Upgrading Type -> e -> TcUpgradeM ()
checkType type_ err = do
    sameType <- isUpgradedType type_
    unless sameType $ diagnosticWithContextF present' err

isUpgradedType :: Upgrading Type -> TcUpgradeM Bool
isUpgradedType type_ = do
    expandedTypes <- runGammaUnderUpgrades (expandTypeSynonyms <$> type_)
    deps <- view upgradingDeps
    let checkSelf = alphaTypeCon
        checkImport t1 t2 =
          case (qualPackage t1, qualPackage t2) of
            (PRImport pkgId1, PRImport pkgId2) ->
              pkgId1 == pkgId2 ||
              Upgrading pkgId1 pkgId2 `elem` deps && removePkgId t1 == removePkgId t2
            _ -> False
        tconCheck t1 t2 = checkSelf t1 t2 || checkImport t1 t2
    pure $ foldU (alphaType' initialAlphaEnv { tconEquivalence = tconCheck }) expandedTypes

isStructurallyEquivalentType :: AlphaEnv -> [TypeConName] -> Upgrading Type -> TcUpgradeM Bool
isStructurallyEquivalentType alphaEnv identicalCons type_ = do
    expandedTypes <- runGammaUnderUpgrades (expandTypeSynonyms <$> type_)
    let checkSelf t1 t2 =
          and
            [ qualPackage t1 == PRSelf
            , qualPackage t2 == PRSelf
            , qualObject t2 `elem` identicalCons
            , alphaTypeCon t1 t2
            ]
        checkImport t1 t2 =
          qualPackage t1 /= PRSelf
          && qualPackage t2 /= PRSelf
          && alphaTypeCon t1 t2
        tconCheck t1 t2 = checkSelf t1 t2 || checkImport t1 t2
    pure $ foldU (alphaType' alphaEnv { tconEquivalence = tconCheck }) expandedTypes

removePkgId :: Qualified a -> Qualified a
removePkgId a = a { qualPackage = PRSelf }

isStructurallyEquivalentDatatype :: [TypeConName] -> Upgrading DefDataType -> TcUpgradeM Bool
isStructurallyEquivalentDatatype identicalCons datatype =
    let params = dataParams <$> datatype
        allKindsMatch = foldU (==) (map snd <$> params)
        paramNames = unsafeFlattenUpgrading (map fst <$> params)
    in
    if not allKindsMatch
      then pure False
      else
        let env = foldl' (flip (foldU bindTypeVar)) initialAlphaEnv paramNames
        in
        case fmap dataCons datatype of
          Upgrading { _past = DataRecord _past, _present = DataRecord _present } -> do
              let allFieldsMatch = map fst _past == map fst _present
              let types = zipWith Upgrading (map snd _past) (map snd _present)
              allTypesMatch <- allM (isStructurallyEquivalentType env identicalCons) types
              pure (allFieldsMatch && allTypesMatch)
          Upgrading { _past = DataVariant _past, _present = DataVariant _present } -> do
              let allConNamesMatch = map fst _past == map fst _present
              let types = zipWith Upgrading (map snd _past) (map snd _present)
              allTypesMatch <- allM (isStructurallyEquivalentType env identicalCons) types
              pure (allConNamesMatch && allTypesMatch)
          Upgrading { _past = DataEnum _past, _present = DataEnum _present } -> do
              pure $ _past == _present
          Upgrading { _past = DataInterface {}, _present = DataInterface {} } ->
              pure False
          _ ->
              pure False

structurallyEqualDataTypes :: HasModules a => Upgrading a -> TcUpgradeM [TypeConName]
structurallyEqualDataTypes hasModules =
  let module_ = NM.toHashMap . getModules <$> hasModules
      (_, existingModules, _) = extractDelExistNew module_
      (_, existingDatatypes, _) = foldMap (extractDelExistNew . fmap (NM.toHashMap . moduleDataTypes)) (HMS.elems existingModules)
  in
  go (HMS.toList existingDatatypes)
  where
    go :: [(TypeConName, Upgrading DefDataType)] -> TcUpgradeM [TypeConName]
    go list = do
      let names = map fst list
      list' <- filterM (isStructurallyEquivalentDatatype names . snd) list
      if names /= map fst list' then go list' else pure names

