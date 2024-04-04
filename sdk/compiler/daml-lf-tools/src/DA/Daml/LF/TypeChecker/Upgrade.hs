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
import           Data.Data
import           Data.Hashable
import qualified Data.HashMap.Strict as HMS
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

checkUpgrade :: Version -> Upgrading LF.Package -> [Diagnostic]
checkUpgrade version package =
    let upgradingWorld = fmap (\package -> emptyGamma (initWorldSelf [] package) version) package
        result =
            runGammaF
                upgradingWorld
                (checkUpgradeM package)
    in
    case result of
      Left err -> [toDiagnostic err]
      Right ((), warnings) -> map toDiagnostic warnings

checkUpgradeM :: Upgrading LF.Package -> TcUpgradeM ()
checkUpgradeM package = do
    (upgradedModules, _new) <- checkDeleted (EUpgradeError . MissingModule . NM.name) $ NM.toHashMap . packageModules <$> package
    forM_ upgradedModules checkModule

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
checkDeleted handleError upgrade = do
    let (deleted, existing, new) = extractDelExistNew upgrade
    throwIfNonEmpty handleError deleted
    pure (existing, new)

throwIfNonEmpty
    :: (Eq k, Hashable k)
    => (a -> Error)
    -> HMS.HashMap k a
    -> TcUpgradeM ()
throwIfNonEmpty handleError hm =
    case HMS.toList hm of
      ((_, first):_) -> throwWithContextF present $ handleError first
      _ -> pure ()

checkModule :: Upgrading LF.Module -> TcUpgradeM ()
checkModule module_ = do
    (existingTemplates, _new) <- checkDeleted (EUpgradeError . MissingTemplate . NM.name) $ NM.toHashMap . moduleTemplates <$> module_
    forM_ existingTemplates $ \template ->
        withContextF
            present
            (ContextTemplate (_present module_) (_present template) TPWhole)
            (checkTemplate module_ template)

    -- checkDeleted should only trigger on datatypes not belonging to templates or choices, which we checked above
    (dtExisting, _dtNew) <- checkDeleted (EUpgradeError . MissingDataCon . NM.name) $ NM.toHashMap . moduleDataTypes <$> module_

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
    withContextF present (ContextTemplate (_present module_) (_present template) TPAgreement) $
        whenDifferent "agreement" (extractFuncFromFuncThis . tplAgreement) template $
            warnWithContextF present $ WTemplateChangedAgreement $ NM.name $ _present template

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

