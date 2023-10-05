-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE ScopedTypeVariables #-}
module DA.Daml.LF.TypeChecker.Upgrade (checkUpgrade, Upgrading(..)) where

--import           DA.Daml.LF.TypeChecker.Env
--import           DA.Daml.LF.TypeChecker.Error
import           DA.Daml.LF.Ast as LF
import           DA.Daml.LF.TypeChecker.Env
import           DA.Daml.LF.TypeChecker.Error
import qualified Data.NameMap as NM
import qualified Data.HashMap.Strict as HMS
import Data.Hashable

import Control.Monad
import Data.Data
import GHC.Generics(Generic)
import Control.DeepSeq

import Development.IDE.Types.Diagnostics

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

zipU :: Upgrading a -> Upgrading b -> (a -> b -> c) -> Upgrading c
zipU a b f = Upgrading { past = f (past a) (past b), present = f (present a) (present b) }

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
      Left err -> [toDiagnostic DsError err]
      Right () -> []

checkUpgradeM :: MonadGamma m => Upgrading LF.Package -> m ()
checkUpgradeM package = do
    (upgradedModules, _new) <- checkDeleted (EUpgradeMissingModule . NM.name) $ NM.toHashMap . packageModules <$> package
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
checkDeleted handle upgrade = do
    let (deleted, existing, new) = extractDelExistNew upgrade
    case HMS.toList deleted of
      ((_, head):_) -> throwWithContext $ handle head
      _ -> pure (existing, new)

checkModule :: MonadGamma m => Upgrading LF.Module -> m ()
checkModule module_ = do
    --(existingDefDataTypes, _new) <- checkDeleted (EUpgradeMissingDataCon . LF.dataTypeCon) $ NM.toHashMap . moduleDataTypes <$> module_
    --forM_ existingDefDataTypes $ \dt ->
    --    withContext
    --        (ContextDefDataType (present module_) (present dt))
    --        (checkDefDataType dt)
    (existingTemplates, _new) <- checkDeleted (EUpgradeMissingTemplate . NM.name) $ NM.toHashMap . moduleTemplates <$> module_
    forM_ existingTemplates $ \template ->
        withContext
            (ContextTemplate (present module_) (present template) TPWhole)
            (checkTemplate module_ template)
    pure ()

lookupDataTypeInMod :: MonadGamma m => Module -> LF.TypeConName -> m DefDataType
lookupDataTypeInMod module_ typeConName =
    let mbDatatype = typeConName `NM.lookup` moduleDataTypes module_
    in
    case mbDatatype of
      Nothing -> throwWithContext (EUnknownDefinition (LEDataType (Qualified PRSelf (moduleName module_) typeConName)))
      Just dt -> pure dt

checkTemplate :: MonadGamma m => Upgrading Module -> Upgrading LF.Template -> m ()
checkTemplate module_ template = do
    definition <- sequence $ zipU module_ template $ \module_ template -> lookupDataTypeInMod module_ (tplTypeCon template)
    checkDefDataType (TemplateBody (tplTypeCon (present template))) definition
    (existingChoices, _new) <- checkDeleted (EUpgradeMissingChoice . NM.name) $ NM.toHashMap . LF.tplChoices <$> template
    forM_ existingChoices $ \choice ->
        withContext (ContextTemplate (present module_) (present template) (TPChoice (present choice))) $ do
            checkCompatibleType (TemplateChoiceInput (chcName (present choice))) module_ (fmap (snd . chcArgBinder) choice)
            checkCompatibleType (TemplateChoiceOutput (chcName (present choice))) module_ (fmap chcReturnType choice)
    pure ()

checkCompatibleType :: MonadGamma m => (LF.TypeConName -> UpgradedRecordOrigin) -> Upgrading Module -> Upgrading LF.Type -> m ()
checkCompatibleType origin module_ type_ = do
    case type_ of
      Upgrading { past = TCon pastName, present = TCon presentName } -> do
          dt <- sequence (zipU module_ (Upgrading { past = qualObject pastName, present = qualObject presentName }) lookupDataTypeInMod)
          checkDefDataType (origin (qualObject presentName)) dt
      -- TODO: Fill out check for more types than zero-argument datatypes
      _ -> pure ()

checkDefDataType :: MonadGamma m => UpgradedRecordOrigin -> Upgrading LF.DefDataType -> m ()
checkDefDataType origin datatype = do
    case fmap dataCons datatype of
      Upgrading { past = DataRecord past, present = DataRecord present } -> checkFields origin (Upgrading {..})
      Upgrading { past = DataVariant {}, present = DataVariant {} } -> pure ()
      Upgrading { past = DataEnum {}, present = DataEnum {} } -> pure ()
      Upgrading { past = DataInterface {}, present = DataInterface {} } -> pure ()
      _ -> throwWithContext (EUpgradeMismatchDataConsVariety (dataTypeCon (past datatype)))

checkFields :: MonadGamma m => UpgradedRecordOrigin -> Upgrading [(FieldName, Type)] -> m ()
checkFields origin fields = do
    (existing, new) <- checkDeleted (const (EUpgradeRecordFieldsMissing origin)) $ HMS.fromList <$> fields

    -- If a field from the upgraded package has had its type changed
    let matchingFieldDifferentType Upgrading{..} = past /= present
    when (any matchingFieldDifferentType existing) $ throwWithContext (EUpgradeRecordFieldsExistingChanged origin)

    -- If a new field has a non-optional type
    let newFieldOptionalType (TOptional _) = True
        newFieldOptionalType _ = False
    when (not (all newFieldOptionalType new)) $ throwWithContext (EUpgradeRecordFieldsNewNonOptional origin)

