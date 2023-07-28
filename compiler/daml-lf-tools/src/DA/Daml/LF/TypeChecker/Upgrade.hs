-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
module DA.Daml.LF.TypeChecker.Upgrade (checkUpgrade, Upgrading(..)) where

--import           DA.Daml.LF.TypeChecker.Env
--import           DA.Daml.LF.TypeChecker.Error
import           DA.Daml.LF.Ast as LF
import           DA.Daml.LF.TypeChecker.Env
import           DA.Daml.LF.TypeChecker.Error
import qualified Data.NameMap as NM
import qualified Data.HashMap.Strict as HMS

import Control.Monad
import Data.Data
import GHC.Generics(Generic)
import Control.DeepSeq

import Development.IDE.Types.Diagnostics

data Upgrading a = Upgrading
    { past :: a
    , present :: a
    }
    deriving (Eq, Data, Generic, NFData, Show, Functor)

instance Functor Upgrading where
    fmap f Upgrading{..} = Upgrading (f past) (f present)

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
    upgradedModules <- checkDeleted packageModules package
    forM_ upgradedModules checkModule

extractDeleted :: NM.Named a => (b -> NM.NameMap a) -> Upgrading b -> HMS.HashMap (NM.Name a) a
extractDeleted extract Upgrading{..} =
    NM.toHashMap (extract past) `HMS.difference` NM.toHashMap (extract present)

extractExisting :: NM.Named a => (b -> NM.NameMap a) -> Upgrading b -> HMS.HashMap (NM.Name a) (Upgrading a)
extractExisting extract Upgrading{..} =
    HMS.intersectionWith Upgrading (NM.toHashMap (extract past)) (NM.toHashMap (extract present))

checkDeleted :: (NM.Named a, MonadGamma m) => (b -> NM.NameMap a) -> Upgrading b -> m (HMS.HashMap (NM.Name a) (Upgrading a))
checkDeleted extract upgrade = do
    let deleted = extractDeleted extract upgrade
    let existing = extractExisting extract upgrade
    if HMS.null deleted
       then pure existing
       else throwWithContext EUpgradeMissing

checkModule :: MonadGamma m => Upgrading LF.Module -> m ()
checkModule module_ = do
    existingDefDataTypes <- checkDeleted moduleDataTypes module_
    forM_ existingDefDataTypes checkDefDataType

checkDefDataType :: MonadGamma m => Upgrading LF.DefDataType -> m ()
checkDefDataType datatype = do
    case fmap dataCons datatype of
      Upgrading { past = DataRecord past, present = DataRecord present } -> checkFields (Upgrading {..})
      Upgrading { past = DataVariant {}, present = DataVariant {} } -> pure ()
      Upgrading { past = DataEnum {}, present = DataEnum {} } -> pure ()
      Upgrading { past = DataInterface {}, present = DataInterface {} } -> pure ()
      _ -> throwWithContext (EUpgradeMismatchDataConsVariety (dataTypeCon (past datatype)))

checkFields :: MonadGamma m => Upgrading [(FieldName, Type)] -> m ()
checkFields fields = do
    let pastMap = HMS.fromList (past fields)
    let presentMap = HMS.fromList (present fields)
    let deletedFields = pastMap `HMS.difference` presentMap
    let matchingFields = HMS.intersectionWith Upgrading pastMap presentMap
    let newFields = presentMap `HMS.difference` pastMap

    -- If a field from the upgraded package is missing
    when (not (HMS.null deletedFields)) $ throwWithContext EUpgradeRecordFieldsMissing

    -- If a field from the upgraded package has had its type changed
    let matchingFieldDifferentType Upgrading{..} = past /= present
    when (any matchingFieldDifferentType matchingFields) $ throwWithContext EUpgradeRecordFieldsExistingChanged

    -- If a new field has a non-optional type
    let newFieldOptionalType (TOptional _) = True
        newFieldOptionalType _ = False
    when (not (all newFieldOptionalType newFields)) $ throwWithContext EUpgradeRecordFieldsNewNonOptional

