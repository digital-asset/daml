-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
module DA.Daml.LF.TypeChecker.Upgrade (checkUpgrade, Upgrading(..)) where

import           Control.DeepSeq
import           Control.Monad (unless, forM_, when)
import           DA.Daml.LF.Ast as LF
import           DA.Daml.LF.Ast.Alpha (alphaExpr, alphaType)
import           DA.Daml.LF.TypeChecker.Env
import           DA.Daml.LF.TypeChecker.Error
import           DA.Daml.LF.Ast.Recursive (TypeF(..))
import           Data.Functor.Foldable (cata)
import           Data.Foldable (fold)
import           Data.Data
import           Data.Hashable
import qualified Data.HashMap.Strict as HMS
import qualified Data.NameMap as NM
import qualified Data.Text as T
import           Development.IDE.Types.Diagnostics
import           GHC.Generics (Generic)

data Upgrading a = Upgrading
    { past :: a
    , present :: a
    }
    deriving (Eq, Data, Generic, NFData, Show)

instance Functor Upgrading where
    fmap f Upgrading{..} = Upgrading (f past) (f present)

instance Foldable Upgrading where
    foldMap f Upgrading{..} = f past <> f present

instance Traversable Upgrading where
    traverse f Upgrading{..} = Upgrading <$> f past <*> f present

instance Applicative Upgrading where
    pure a = Upgrading a a
    (<*>) f a = Upgrading { past = past f (past a), present = present f (present a) }

foldU :: (a -> a -> b) -> Upgrading a -> b
foldU f u = f (past u) (present u)

checkUpgrade :: Version -> Upgrading LF.Package -> [Diagnostic]
checkUpgrade version package =
    let result =
            runGamma
                (initWorldSelf [] (present package))
                version
                (checkUpgradeM package)
    in
    case result of
      Left err -> [toDiagnostic err]
      Right ((), warnings) -> map toDiagnostic warnings

checkUpgradeM :: MonadGamma m => Upgrading LF.Package -> m ()
checkUpgradeM package = do
    (upgradedModules, _new) <- checkDeleted (EUpgradeError . MissingModule . NM.name) $ NM.toHashMap . packageModules <$> package
    forM_ upgradedModules checkModule

extractDelExistNew
    :: (Eq k, Hashable k)
    => Upgrading (HMS.HashMap k a)
    -> (HMS.HashMap k a, HMS.HashMap k (Upgrading a), HMS.HashMap k a)
extractDelExistNew Upgrading{..} =
    ( past `HMS.difference` present
    , HMS.intersectionWith Upgrading past present
    , present `HMS.difference` past
    )

checkDeleted
    :: (Eq k, Hashable k, MonadGamma m)
    => (a -> Error)
    -> Upgrading (HMS.HashMap k a)
    -> m (HMS.HashMap k (Upgrading a), HMS.HashMap k a)
checkDeleted handleError upgrade = do
    let (deleted, existing, new) = extractDelExistNew upgrade
    throwIfNonEmpty handleError deleted
    pure (existing, new)

throwIfNonEmpty
    :: (Eq k, Hashable k, MonadGamma m)
    => (a -> Error)
    -> HMS.HashMap k a
    -> m ()
throwIfNonEmpty handleError hm =
    case HMS.toList hm of
      ((_, first):_) -> throwWithContext $ handleError first
      _ -> pure ()

checkModule :: MonadGamma m => Upgrading LF.Module -> m ()
checkModule module_ = do
    (existingTemplates, _new) <- checkDeleted (EUpgradeError . MissingTemplate . NM.name) $ NM.toHashMap . moduleTemplates <$> module_
    forM_ existingTemplates $ \template ->
        withContext
            (ContextTemplate (present module_) (present template) TPWhole)
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
        allChoiceReturnTCons :: LF.Module -> HMS.HashMap LF.TypeConName (LF.Template, LF.TemplateChoice)
        allChoiceReturnTCons module_ = HMS.fromList $ do
            template <- NM.toList (moduleTemplates module_)
            choice <- NM.toList (tplChoices template)
            dtName <- flip cata (chcReturnType choice) $ \case
                TConF dtName -> [dtName]
                rest -> fold rest
            pure (qualObject dtName, (template, choice))
        dataTypeOrigin
            :: DefDataType -> Module
            -> (UpgradedRecordOrigin, (Context, Bool))
        dataTypeOrigin dt module_
            | Just template <- NM.name dt `NM.lookup` moduleTemplates module_ =
                ( TemplateBody (NM.name dt)
                , ( ContextTemplate module_ template TPWhole
                  , True
                  )
                )
            | Just (template, choice) <- NM.name dt `HMS.lookup` deriveChoiceInfo module_ =
                ( TemplateChoiceInput (NM.name template) (NM.name choice)
                , ( ContextTemplate module_ template (TPChoice choice)
                  , True
                  )
                )
            | otherwise =
                ( TopLevel
                , (ContextDefDataType module_ dt
                  , NM.name dt `HMS.member` allChoiceReturnTCons module_
                  )
                )

    forM_ dtExisting $ \dt ->
        -- Get origin/context for each datatype in both past and present
        let origin = dataTypeOrigin <$> dt <*> module_
        in
        -- If origins don't match, record has changed origin
        if foldU (/=) (fst <$> origin) then
            withContext (ContextDefDataType (present module_) (present dt)) $
                throwWithContext (EUpgradeError (RecordChangedOrigin (dataTypeCon (present dt)) (fst (past origin)) (fst (present origin))))
        else
            let (presentOrigin, (context, shouldCheck)) = present origin
            in
            when shouldCheck $
                case checkDefDataType presentOrigin dt of
                  Nothing -> pure ()
                  Just e -> withContext context $ throwWithContext e

checkTemplate :: forall m. MonadGamma m => Upgrading Module -> Upgrading LF.Template -> m ()
checkTemplate module_ template = do
    -- Check that no choices have been removed
    (existingChoices, _existingNew) <- checkDeleted (EUpgradeError . MissingChoice . NM.name) $ NM.toHashMap . tplChoices <$> template
    forM_ existingChoices $ \choice -> do
        withContext (ContextTemplate (present module_) (present template) (TPChoice (present choice))) $ do
            let returnTypesMatch = foldU alphaType (fmap chcReturnType choice)
            unless returnTypesMatch $
                throwWithContext (EUpgradeError (ChoiceChangedReturnType (NM.name (present choice))))

            whenDifferent "controllers" (extractFuncFromFuncThisArg . chcControllers) choice $
                warnWithContext $ WChoiceChangedControllers $ NM.name $ present choice

            let observersErr = WChoiceChangedObservers $ NM.name $ present choice
            case fmap (mapENilToNothing . chcObservers) choice of
               Upgrading { past = Nothing, present = Nothing } -> do
                   pure ()
               Upgrading { past = Just past, present = Just present } -> do
                   whenDifferent "observers"
                       extractFuncFromFuncThisArg (Upgrading past present)
                       (warnWithContext observersErr)
               _ -> do
                   warnWithContext observersErr

            let authorizersErr = WChoiceChangedAuthorizers $ NM.name $ present choice
            case fmap (mapENilToNothing . chcAuthorizers) choice of
               Upgrading { past = Nothing, present = Nothing } -> pure ()
               Upgrading { past = Just past, present = Just present } ->
                   whenDifferent "authorizers"
                       extractFuncFromFuncThisArg (Upgrading past present)
                       (warnWithContext authorizersErr)
               _ -> warnWithContext authorizersErr
        pure choice

    -- This check assumes that we encode signatories etc. on a template as
    -- $<uniquename> this, where $<uniquename> is a function that contains the
    -- actual definition. We resolve this function and check that it is
    -- identical.
    withContext (ContextTemplate (present module_) (present template) TPPrecondition) $
        whenDifferent "precondition" (extractFuncFromCaseFuncThis . tplPrecondition) template $
            warnWithContext $ WTemplateChangedPrecondition $ NM.name $ present template
    withContext (ContextTemplate (present module_) (present template) TPSignatories) $
        whenDifferent "signatories" (extractFuncFromFuncThis . tplSignatories) template $
            warnWithContext $ WTemplateChangedSignatories $ NM.name $ present template
    withContext (ContextTemplate (present module_) (present template) TPObservers) $
        whenDifferent "observers" (extractFuncFromFuncThis . tplObservers) template $
            warnWithContext $ WTemplateChangedObservers $ NM.name $ present template

    withContext (ContextTemplate (present module_) (present template) TPKey) $ do
        case fmap tplKey template of
           Upgrading { past = Nothing, present = Nothing } -> do
               pure ()
           Upgrading { past = Just pastKey, present = Just presentKey } -> do
               let tplKey = Upgrading pastKey presentKey

               -- Key type musn't change
               let keyTypesMatch = foldU alphaType (fmap tplKeyType tplKey)
               unless keyTypesMatch $
                   throwWithContext (EUpgradeError (TemplateChangedKeyType (NM.name (present template))))

               -- But expression for computing it may
               whenDifferent "key expression"
                   (extractFuncFromFuncThis . tplKeyBody) tplKey
                   (warnWithContext $ WTemplateChangedKeyExpression $ NM.name $ present template)
               whenDifferent "key maintainers"
                   (extractFuncFromTyAppNil . tplKeyMaintainers) tplKey
                   (warnWithContext $ WTemplateChangedKeyMaintainers $ NM.name $ present template)
           Upgrading { past = Just pastKey, present = Nothing } ->
               throwWithContext $ EUpgradeError $ TemplateRemovedKey (NM.name (present template)) pastKey
           Upgrading { past = Nothing, present = Just presentKey } ->
               warnWithContext $ WTemplateAddedKeyDefinition (NM.name (present template)) presentKey

    -- TODO: Check that return type of a choice is compatible
    pure ()
    where
        mapENilToNothing :: Maybe Expr -> Maybe Expr
        mapENilToNothing (Just (LF.ENil (LF.TBuiltin LF.BTParty))) = Nothing
        mapENilToNothing e = e

        -- Given an extractor from the list below, whenDifferent runs an action
        -- when the relevant expressions differ.
        whenDifferent :: Show a => String -> (a -> Module -> Either String Expr) -> Upgrading a -> m () -> m ()
        whenDifferent field extractor exprs act =
            let resolvedWithPossibleError = sequence $ extractor <$> exprs <*> module_
            in
            case resolvedWithPossibleError of
                Left err ->
                    warnWithContext (WCouldNotExtractForUpgradeChecking (T.pack field) (Just (T.pack err)))
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

checkDefDataType :: UpgradedRecordOrigin -> Upgrading LF.DefDataType -> Maybe Error
checkDefDataType origin datatype = do
    case fmap dataCons datatype of
      Upgrading { past = DataRecord past, present = DataRecord present } -> checkFields origin (Upgrading {..})
      Upgrading { past = DataVariant {}, present = DataVariant {} } -> Nothing
      Upgrading { past = DataEnum {}, present = DataEnum {} } -> Nothing
      Upgrading { past = DataInterface {}, present = DataInterface {} } -> Nothing
      _ -> Just (EUpgradeError (MismatchDataConsVariety (dataTypeCon (past datatype))))

checkFields :: UpgradedRecordOrigin -> Upgrading [(FieldName, Type)] -> Maybe Error
checkFields origin fields =
    let (deleted, existing, new) = extractDelExistNew $ HMS.fromList <$> fields
    in
    if not (HMS.null deleted) then
        Just (EUpgradeError (RecordFieldsMissing origin))
    -- If a field from the upgraded package has had its type changed
    else if any matchingFieldDifferentType existing then
        Just (EUpgradeError (RecordFieldsExistingChanged origin))
    -- If a new field has a non-optional type
    else if not (all newFieldOptionalType new) then
        Just (EUpgradeError (RecordFieldsNewNonOptional origin))
    else
        Nothing
    where
        matchingFieldDifferentType Upgrading{..} = past /= present
        newFieldOptionalType (TOptional _) = True
        newFieldOptionalType _ = False
