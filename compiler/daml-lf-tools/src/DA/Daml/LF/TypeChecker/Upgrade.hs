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
    (upgradedModules, _new) <- checkDeleted (const EUpgradeMissing) $ NM.toHashMap . packageModules <$> package
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
    (existingDefDataTypes, _new) <- checkDeleted (const EUpgradeMissing) $ NM.toHashMap . moduleDataTypes <$> module_
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
    (existing, new) <- checkDeleted (const EUpgradeRecordFieldsMissing) $ HMS.fromList <$> fields

    -- If a field from the upgraded package has had its type changed
    let matchingFieldDifferentType Upgrading{..} = past /= present
    when (any matchingFieldDifferentType existing) $ throwWithContext EUpgradeRecordFieldsExistingChanged

    -- If a new field has a non-optional type
    let newFieldOptionalType (TOptional _) = True
        newFieldOptionalType _ = False
    when (not (all newFieldOptionalType new)) $ throwWithContext EUpgradeRecordFieldsNewNonOptional

