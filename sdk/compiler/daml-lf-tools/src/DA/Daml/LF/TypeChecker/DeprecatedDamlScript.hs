-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker.DeprecatedDamlScript (
        module DA.Daml.LF.TypeChecker.DeprecatedDamlScript
    ) where

--import Data.Functor.Foldable
--import Control.Monad
import DA.Daml.LF.Ast
import DA.Daml.LF.TypeChecker.Env
--import DA.Daml.LF.TypeChecker.Error
--import DA.Daml.LF.Ast.Recursive

--isNewDamlScript :: PackageName -> Bool
--isNewDamlScript name = name `elem` map PackageName ["daml3-script", "daml-script-lts", "daml-script-lts-stable"]
--
--checkTypeCon :: (MonadGamma m) => Qualified TypeConName -> m ()
--checkTypeCon name = do
--  pkg <- inWorld (lookupPackage (qualPackage name))
--
--  -- When depending on a datatype from a new (>= LF1.17) daml-script
--  case packageMetadata pkg of
--    Nothing -> pure ()
--    Just meta@PackageMetadata { packageName } -> do
--      when (isNewDamlScript packageName) $ do
--        diagnosticWithContext $ WEDependsOnDatatypeFromNewDamlScript (packageId ) name
--
--  -- When depending on an LF1.15 serializable datatype while targeting >= LF1.17
--  targetLfVersion <- getLfVersion
--  datatype <- inWorld (lookupDataType name)
--  when (packageLfVersion pkg == version1_15 && targetLfVersion `supports` featurePackageUpgrades && getIsSerializable (dataSerializable datatype)) $ do
--    diagnosticWithContext $ WEUpgradeDependsOnSerializableNonUpgradeableDataType name
--
--checkTyp :: forall m. MonadGamma m => Type -> m ()
--checkTyp typ = do
--    _ <- cataA go typ
--    pure ()
--  where
--    go :: TypeF (m Type) -> m Type
--    go typeF = do
--      case typeF of
--        TConF tcn -> checkTypeCon tcn
--        _ -> pure ()
--      embed <$> sequence typeF
--
--checkModule :: forall m. MonadGamma m => Module -> m ()
--checkModule mod0 = do
--  forM_ (moduleDataTypes mod0) $ \dt -> do
--    case dataCons dt of
--      DataRecord fields -> forM_ fields $ \(_, typ) ->
--        checkTyp typ
--      DataVariant constructors -> forM_ constructors $ \(_, typ) ->
--        checkTyp typ
--      DataEnum _ -> pure ()
--      DataInterface -> pure ()
--  pure ()

checkModule :: forall m. MonadGamma m => Module -> m ()
checkModule _ = pure ()
