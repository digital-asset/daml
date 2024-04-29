-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE TemplateHaskell #-}

module DA.Daml.LF.TypeChecker.Upgrade (
        module DA.Daml.LF.TypeChecker.Upgrade
    ) where

import           Control.DeepSeq
import           Control.Monad (unless, forM_, when)
import           Control.Monad.Reader (withReaderT)
import           Control.Lens hiding (Context)
import           DA.Daml.LF.Ast as LF
import           DA.Daml.LF.Ast.Alpha (alphaExpr, alphaType)
import           DA.Daml.LF.TypeChecker.Check (expandTypeSynonyms)
import           DA.Daml.LF.TypeChecker.Env
import           DA.Daml.LF.TypeChecker.Error
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

-- Allows us to split the world into upgraded and non-upgraded
type TcUpgradeM = TcMF (Upgrading Gamma)

runGammaUnderUpgrades :: Upgrading (TcM a) -> TcUpgradeM (Upgrading a)
runGammaUnderUpgrades Upgrading{ _past = pastAction, _present = presentAction } = do
    pastResult <- withReaderT _past pastAction
    presentResult <- withReaderT _present presentAction
    pure Upgrading { _past = pastResult, _present = presentResult }

checkUpgrade :: Version -> Bool -> LF.Package -> Maybe (LF.PackageId, LF.Package) -> [Diagnostic]
checkUpgrade version shouldTypecheckUpgrades presentPkg mbUpgradedPackage =
    let bothPkgDiagnostics :: Either Error ((), [Warning])
        bothPkgDiagnostics =
            case mbUpgradedPackage of
                Nothing ->
                    Right ((), [])
                Just (_, pastPkg) ->
                    let package = Upgrading { _past = pastPkg, _present = presentPkg }
                        upgradingWorld = fmap (\package -> emptyGamma (initWorldSelf [] package) version) package
                    in
                    runGammaF upgradingWorld $ do
                        when shouldTypecheckUpgrades (checkUpgradeM package)

        singlePkgDiagnostics :: Either Error ((), [Warning])
        singlePkgDiagnostics =
            let world = initWorldSelf [] presentPkg
            in
            runGamma world version $ do
                 checkNewInterfacesHaveNoTemplates presentPkg
                 checkNewInterfacesAreUnused presentPkg

        extractDiagnostics :: Either Error ((), [Warning]) -> [Diagnostic]
        extractDiagnostics result =
            case result of
              Left err -> [toDiagnostic err]
              Right ((), warnings) -> map toDiagnostic warnings
    in
    extractDiagnostics bothPkgDiagnostics ++ extractDiagnostics singlePkgDiagnostics

checkUpgradeM :: Upgrading LF.Package -> TcUpgradeM ()
checkUpgradeM package = do
    (upgradedModules, _new) <- checkDeleted (EUpgradeError . MissingModule . NM.name) $ NM.toHashMap . packageModules <$> package
    forM_ upgradedModules $ \module_ -> checkModule package module_

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
    :: (Eq k, Hashable k)
    => (a -> Error)
    -> Upgrading (HMS.HashMap k a)
    -> TcUpgradeM (HMS.HashMap k (Upgrading a), HMS.HashMap k a)
checkDeleted handleError upgrade =
    checkDeletedG ((Nothing,) . handleError) upgrade

checkDeletedWithContext
    :: (Eq k, Hashable k)
    => (a -> (Context, Error))
    -> Upgrading (HMS.HashMap k a)
    -> TcUpgradeM (HMS.HashMap k (Upgrading a), HMS.HashMap k a)
checkDeletedWithContext handleError upgrade =
    checkDeletedG (first Just . handleError) upgrade

checkDeletedG
    :: (Eq k, Hashable k)
    => (a -> (Maybe Context, Error))
    -> Upgrading (HMS.HashMap k a)
    -> TcUpgradeM (HMS.HashMap k (Upgrading a), HMS.HashMap k a)
checkDeletedG handleError upgrade = do
    let (deleted, existing, new) = extractDelExistNew upgrade
    throwIfNonEmpty handleError deleted
    pure (existing, new)

throwIfNonEmpty
    :: (Eq k, Hashable k)
    => (a -> (Maybe Context, Error))
    -> HMS.HashMap k a
    -> TcUpgradeM ()
throwIfNonEmpty handleError hm =
    case HMS.toList hm of
      ((_, first):_) ->
          let (ctx, err) = handleError first
              ctxHandler =
                  case ctx of
                    Nothing -> id
                    Just ctx -> withContextF present ctx
          in
          ctxHandler $ throwWithContextF present err
      _ -> pure ()

checkModule :: Upgrading LF.Package -> Upgrading LF.Module -> TcUpgradeM ()
checkModule package module_ = do
    (existingTemplates, _new) <- checkDeleted (EUpgradeError . MissingTemplate . NM.name) $ NM.toHashMap . moduleTemplates <$> module_
    forM_ existingTemplates $ \template ->
        withContextF
            present
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
    (_instanceExisting, instanceNew) <-
        checkDeletedWithContext
            (\(tpl, impl) ->
                ( ContextTemplate (_present module_) tpl TPWhole
                , EUpgradeError (MissingImplementation (NM.name tpl) (LF.qualObject (NM.name impl)))
                ))
            (flattenInstances <$> module_)
    checkUpgradedInterfacesAreUnused (_present package) (_present module_) instanceNew

    -- checkDeleted should only trigger on datatypes not belonging to templates or choices or interfaces, which we checked above
    (dtExisting, _dtNew) <- checkDeleted (EUpgradeError . MissingDataCon . NM.name) unownedDts

    forM_ dtExisting $ \dt ->
        -- Get origin/context for each datatype in both _past and _present
        let origin = dataTypeOrigin <$> dt <*> module_
        in
        -- If origins don't match, record has changed origin
        if foldU (/=) (fst <$> origin) then
            withContextF present (ContextDefDataType (_present module_) (_present dt)) $
                throwWithContextF present (EUpgradeError (RecordChangedOrigin (dataTypeCon (_present dt)) (fst (_past origin)) (fst (_present origin))))
        else do
            let (presentOrigin, context) = _present origin
            withContextF present context $ checkDefDataType presentOrigin dt

-- It is always invalid to keep an interface in an upgrade
checkContinuedIfaces
    :: Upgrading Module
    -> HMS.HashMap LF.TypeConName (Upgrading (DefDataType, DefInterface))
    -> TcUpgradeM ()
checkContinuedIfaces module_ ifaces =
    forM_ ifaces $ \upgradedDtIface ->
        let (_dt, iface) = _present upgradedDtIface
        in
        withContextF present (ContextDefInterface (_present module_) iface IPWhole) $
            throwWithContextF present $ EUpgradeError $ TriedToUpgradeIface (NM.name iface)

-- This warning should run even when no upgrade target is set
checkNewInterfacesHaveNoTemplates :: LF.Package -> TcM ()
checkNewInterfacesHaveNoTemplates presentPkg =
    let modules = NM.toHashMap $ packageModules presentPkg
        templateDefined = HMS.filter (not . NM.null . moduleTemplates) modules
        interfaceDefined = HMS.filter (not . NM.null . moduleInterfaces) modules
        templateAndInterfaceDefined =
            HMS.intersectionWith (,) templateDefined interfaceDefined
    in
    forM_ (HMS.toList templateAndInterfaceDefined) $ \(_, (module_, _)) ->
        withContext (ContextDefModule module_) $
            warnWithContext WShouldDefineIfacesAndTemplatesSeparately

-- This warning should run even when no upgrade target is set
checkNewInterfacesAreUnused :: LF.Package -> TcM ()
checkNewInterfacesAreUnused presentPkg =
    forM_ definedAndInstantiated $ \((module_, iface), implementations) ->
        withContext (ContextDefInterface module_ iface IPWhole) $
            warnWithContext $ WShouldDefineIfaceWithoutImplementation (NM.name iface) ((\(_,a,_) -> NM.name a) <$> implementations)
    where
    definedIfaces :: HMS.HashMap (LF.Qualified LF.TypeConName) (Module, DefInterface)
    definedIfaces = HMS.unions
        [ HMS.mapKeys qualify $ HMS.map (module_,) $ NM.toHashMap (moduleInterfaces module_)
        | module_ <- NM.elems (packageModules presentPkg)
        , let qualify :: LF.TypeConName -> LF.Qualified LF.TypeConName
              qualify tcn = Qualified PRSelf (NM.name module_) tcn
        ]

    definedAndInstantiated
        :: HMS.HashMap
            (LF.Qualified LF.TypeConName)
            ((Module, DefInterface), [(Module, Template, TemplateImplements)])
    definedAndInstantiated = HMS.intersectionWith (,) definedIfaces (instantiatedIfaces presentPkg)

checkUpgradedInterfacesAreUnused
    :: Package
    -> Module
    -> HMS.HashMap (TypeConName, Qualified TypeConName) (Template, TemplateImplements)
    -> TcUpgradeM ()
checkUpgradedInterfacesAreUnused package module_ newInstances = do
    forM_ (HMS.toList newInstances) $ \((tplName, ifaceName), (tpl, implementation)) ->
        when (fromUpgradedPackage ifaceName) $
            let qualifiedTplName = Qualified PRSelf (moduleName module_) tplName
                ifaceInstanceHead = InterfaceInstanceHead ifaceName qualifiedTplName
            in
            withContextF present (ContextTemplate module_ tpl (TPInterfaceInstance ifaceInstanceHead Nothing)) $
                warnWithContextF present $ WShouldDefineTplInSeparatePackage (NM.name tpl) (LF.qualObject (NM.name implementation))
    where
    fromUpgradedPackage :: forall a. LF.Qualified a -> Bool
    fromUpgradedPackage identifier =
        case (upgradedPackageId (packageMetadata package), qualPackage identifier) of
          (Just upgradedId, PRImport identifierOrigin) -> upgradedId == identifierOrigin
          _ -> False

instantiatedIfaces :: LF.Package -> HMS.HashMap (LF.Qualified LF.TypeConName) [(Module, Template, TemplateImplements)]
instantiatedIfaces pkg = foldl' (HMS.unionWith (<>)) HMS.empty $ (map . fmap) pure
    [ HMS.map (module_,template,) $ NM.toHashMap $ tplImplements template
    | module_ <- NM.elems (packageModules pkg)
    , template <- NM.elems (moduleTemplates module_)
    ]

checkTemplate :: Upgrading Module -> Upgrading LF.Template -> TcUpgradeM ()
checkTemplate module_ template = do
    -- Check that no choices have been removed
    (existingChoices, _existingNew) <- checkDeleted (EUpgradeError . MissingChoice . NM.name) $ NM.toHashMap . tplChoices <$> template
    forM_ existingChoices $ \choice -> do
        withContextF present (ContextTemplate (_present module_) (_present template) (TPChoice (_present choice))) $ do
            checkUpgradeType (fmap chcReturnType choice)
                (EUpgradeError (ChoiceChangedReturnType (NM.name (_present choice))))

            whenDifferent "controllers" (extractFuncFromFuncThisArg . chcControllers) choice $
                warnWithContextF present $ WChoiceChangedControllers $ NM.name $ _present choice

            let observersErr = WChoiceChangedObservers $ NM.name $ _present choice
            case fmap (mapENilToNothing . chcObservers) choice of
               Upgrading { _past = Nothing, _present = Nothing } -> do
                   pure ()
               Upgrading { _past = Just _past, _present = Just _present } -> do
                   whenDifferent "observers"
                       extractFuncFromFuncThisArg (Upgrading _past _present)
                       (warnWithContextF present observersErr)
               _ -> do
                   warnWithContextF present observersErr

            let authorizersErr = WChoiceChangedAuthorizers $ NM.name $ _present choice
            case fmap (mapENilToNothing . chcAuthorizers) choice of
               Upgrading { _past = Nothing, _present = Nothing } -> pure ()
               Upgrading { _past = Just _past, _present = Just _present } ->
                   whenDifferent "authorizers"
                       extractFuncFromFuncThisArg (Upgrading _past _present)
                       (warnWithContextF present authorizersErr)
               _ -> warnWithContextF present authorizersErr
        pure choice

    -- This check assumes that we encode signatories etc. on a template as
    -- $<uniquename> this, where $<uniquename> is a function that contains the
    -- actual definition. We resolve this function and check that it is
    -- identical.
    withContextF present (ContextTemplate (_present module_) (_present template) TPPrecondition) $
        whenDifferent "precondition" (extractFuncFromCaseFuncThis . tplPrecondition) template $
            warnWithContextF present $ WTemplateChangedPrecondition $ NM.name $ _present template
    withContextF present (ContextTemplate (_present module_) (_present template) TPSignatories) $
        whenDifferent "signatories" (extractFuncFromFuncThis . tplSignatories) template $
            warnWithContextF present $ WTemplateChangedSignatories $ NM.name $ _present template
    withContextF present (ContextTemplate (_present module_) (_present template) TPObservers) $
        whenDifferent "observers" (extractFuncFromFuncThis . tplObservers) template $
            warnWithContextF present $ WTemplateChangedObservers $ NM.name $ _present template

    withContextF present (ContextTemplate (_present module_) (_present template) TPKey) $ do
        case fmap tplKey template of
           Upgrading { _past = Nothing, _present = Nothing } -> do
               pure ()
           Upgrading { _past = Just pastKey, _present = Just presentKey } -> do
               let tplKey = Upgrading pastKey presentKey

               -- Key type musn't change
               checkUpgradeType (fmap tplKeyType tplKey)
                   (EUpgradeError (TemplateChangedKeyType (NM.name (_present template))))

               -- But expression for computing it may
               whenDifferent "key expression"
                   (extractFuncFromFuncThis . tplKeyBody) tplKey
                   (warnWithContextF present $ WTemplateChangedKeyExpression $ NM.name $ _present template)
               whenDifferent "key maintainers"
                   (extractFuncFromTyAppNil . tplKeyMaintainers) tplKey
                   (warnWithContextF present $ WTemplateChangedKeyMaintainers $ NM.name $ _present template)
           Upgrading { _past = Just pastKey, _present = Nothing } ->
               throwWithContextF present $ EUpgradeError $ TemplateRemovedKey (NM.name (_present template)) pastKey
           Upgrading { _past = Nothing, _present = Just presentKey } ->
               throwWithContextF present $ EUpgradeError $ TemplateAddedKey (NM.name (_present template)) presentKey

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
                    warnWithContextF present (WCouldNotExtractForUpgradeChecking (T.pack field) (Just (T.pack err)))
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
          (existing, _new) <- checkDeleted (\_ -> EUpgradeError (VariantRemovedVariant origin)) (fmap HMS.fromList upgrade)
          when (not $ and $ foldU (zipWith (==)) $ fmap (map fst) upgrade) $
              throwWithContextF present (EUpgradeError (VariantVariantsOrderChanged origin))
          different <- filterHashMapM (fmap not . isSameType) existing
          when (not (null different)) $
              throwWithContextF present $ EUpgradeError (VariantChangedVariantType origin)
      Upgrading { _past = DataEnum _past, _present = DataEnum _present } -> do
          let upgrade = Upgrading{..}
          (_, _new) <-
              checkDeleted
                (\_ -> EUpgradeError (EnumRemovedVariant origin))
                (fmap (HMS.fromList . map (,())) upgrade)
          when (not $ and $ foldU (zipWith (==)) upgrade) $
              throwWithContextF present (EUpgradeError (EnumVariantsOrderChanged origin))
      Upgrading { _past = DataInterface {}, _present = DataInterface {} } ->
          pure ()
      _ ->
          throwWithContextF present (EUpgradeError (MismatchDataConsVariety (dataTypeCon (_past datatype))))

filterHashMapM :: (Applicative m) => (a -> m Bool) -> HMS.HashMap k a -> m (HMS.HashMap k a)
filterHashMapM pred t =
    fmap fst . HMS.filter snd <$> traverse (\x -> (x,) <$> pred x) t

checkFields :: UpgradedRecordOrigin -> Upgrading [(FieldName, Type)] -> TcUpgradeM ()
checkFields origin fields = do
    (existing, new) <- checkDeleted (\_ -> EUpgradeError (RecordFieldsMissing origin)) (fmap HMS.fromList fields)
    -- If a field from the upgraded package has had its type changed
    different <- filterHashMapM (fmap not . isSameType) existing
    when (not (HMS.null different)) $
        throwWithContextF present (EUpgradeError (RecordFieldsExistingChanged origin))
    when (not (all newFieldOptionalType new)) $
        case origin of
          VariantConstructor{} ->
            throwWithContextF present (EUpgradeError (VariantAddedVariantField origin))
          _ ->
            throwWithContextF present (EUpgradeError (RecordFieldsNewNonOptional origin))
        -- If a new field has a non-optional type
    -- If the order of fields changed
    when (not $ and $ foldU (zipWith (==)) $ fmap (map fst) fields) $
        throwWithContextF present (EUpgradeError (RecordFieldsOrderChanged origin))
    where
        newFieldOptionalType (TOptional _) = True
        newFieldOptionalType _ = False

-- Check type upgradability
checkUpgradeType :: Upgrading Type -> Error -> TcUpgradeM ()
checkUpgradeType type_ err = do
    sameType <- isSameType type_
    unless sameType $ throwWithContextF present err

isSameType :: Upgrading Type -> TcUpgradeM Bool
isSameType type_ = do
    expandedTypes <- runGammaUnderUpgrades (expandTypeSynonyms <$> type_)
    let strippedIdentifiers = fmap unifyTypes expandedTypes
    pure (foldU alphaType strippedIdentifiers)

unifyIdentifier :: Qualified a -> Qualified a
unifyIdentifier q = q { qualPackage = PRSelf }

unifyTypes :: Type -> Type
unifyTypes typ =
    case typ of
      TNat n -> TNat n
      TSynApp n args -> TSynApp (unifyIdentifier n) (map unifyTypes args)
      TVar n -> TVar n
      TCon con -> TCon (unifyIdentifier con)
      TBuiltin bt -> TBuiltin bt
      TApp fun arg -> TApp (unifyTypes fun) (unifyTypes arg)
      TForall v body -> TForall v (unifyTypes body)
      TStruct fields -> TStruct ((map . fmap) unifyTypes fields)

