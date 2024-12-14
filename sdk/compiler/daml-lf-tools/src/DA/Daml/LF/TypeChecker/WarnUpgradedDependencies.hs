-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TypeFamilies #-}

-- | This module provides functions to perform the Daml-LF constraint checks on
-- types in certain positions. In fact, we also need to do some form of
-- "constraint inference". To perform this inference in an incremental fashion,
-- we also provide a way to augment a 'ModuleInterface' with constraint
-- information about the exported data types in 'augmentInterface'.
--
-- Checking whether a function or template definition complies with the Daml-LF
-- type constraints is straightforward. It is implemented in 'checkModule', which
-- assumes that the constraint information on the data types in the module being
-- checked have already been inferred. In other words, the environment must
-- contain the 'ModuleInterface' produced by 'augmentInterface'.
module DA.Daml.LF.TypeChecker.WarnUpgradedDependencies
  ( checkModule
  ) where

import           Control.Monad.Extra
import           Data.Foldable (for_)

import DA.Daml.LF.Ast
import DA.Daml.LF.TypeChecker.Env
import DA.Daml.LF.TypeChecker.Error
import Data.Functor.Foldable (cataA, embed)
import DA.Daml.LF.Ast.Recursive (TypeF(..))

-- | Check whether a type refers to 
checkType :: forall m. MonadGamma m => Bool -> Type -> m ()
checkType isSerializable typ = do
    _ <- cataA go typ
    pure ()
  where
    go :: TypeF (m Type) -> m Type
    go typeF = do
      case typeF of
        TConF tcn -> checkTypeCon isSerializable tcn
        _ -> pure ()
      embed <$> sequence typeF

checkTypeCon :: (MonadGamma m) => Bool -> Qualified TypeConName -> m ()
checkTypeCon isSerializable name = do
  case qualPackage name of
    SelfPackageId -> pure ()
    ImportedPackageId pkgId ->
      when isSerializable $ do
        -- When using a datatype from a new (>= LF1.17) daml-script in a
        -- serializable position while targeting >= LF1.17
        pkg <- inWorld (lookupExternalPackage pkgId)
        let meta = packageMetadata pkg
            PackageMetadata { packageName } = meta
        let isNewDamlScript :: Bool
            isNewDamlScript = packageName `elem` map PackageName ["daml3-script", "daml-script-lts", "daml-script-lts-stable"]
        when isNewDamlScript $ do
          diagnosticWithContext $ WEDependsOnDatatypeFromNewDamlScript (pkgId, meta) (packageLfVersion pkg) name

-- | Check whether a data type definition satisfies all serializability constraints.
checkDataType :: MonadGamma m => DefDataType -> m ()
checkDataType dataType = do
  let isSerializable = getIsSerializable (dataSerializable dataType)
  case dataCons dataType of
    DataEnum {} -> pure ()
    DataInterface {} -> pure ()
    DataVariant constructors -> forM_ constructors (checkType isSerializable . snd)
    DataRecord fields -> forM_ fields (checkType isSerializable . snd)
  pure ()

-- | Check whether a template satisfies all serializability constraints.
checkTemplate :: MonadGamma m => Module -> Template -> m ()
checkTemplate mod0 tpl = do
  let tcon = Qualified SelfPackageId (moduleName mod0) (tplTypeCon tpl)
  checkType True (TCon tcon)
  for_ (tplChoices tpl) $ \ch -> withContext (ContextTemplate mod0 tpl $ TPChoice ch) $ do
    checkType True (snd (chcArgBinder ch))
    checkType True (chcReturnType ch)
  for_ (tplKey tpl) $ \key -> withContext (ContextTemplate mod0 tpl TPKey) $ do
    checkType True (tplKeyType key)

-- | Check whether a template satisfies all serializability constraints.
checkInterface :: MonadGamma m => Module -> DefInterface -> m ()
checkInterface mod0 iface = do
  for_ (intChoices iface) $ \ch -> do
    withContext (ContextDefInterface mod0 iface (IPChoice ch)) $ do
      checkType True (snd (chcArgBinder ch))
      checkType True (chcReturnType ch)

  checkType True $ intView iface

-- | Check whether exception is serializable.
checkException :: MonadGamma m => Module -> DefException -> m ()
checkException mod0 exn = do
    let tcon = Qualified SelfPackageId (moduleName mod0) (exnName exn)
    checkType True (TCon tcon)

-- | Check whether a module satisfies all serializability constraints.
checkModule :: MonadGamma m => Module -> m ()
checkModule mod0 = do
  for_ (moduleDataTypes mod0) $ \dataType ->
    withContext (ContextDefDataType mod0 dataType) $
      checkDataType dataType
  for_ (moduleTemplates mod0) $ \tpl ->
    withContext (ContextTemplate mod0 tpl TPWhole) $
      checkTemplate mod0 tpl
  for_ (moduleExceptions mod0) $ \exn ->
    withContext (ContextDefException mod0 exn) $
      checkException mod0 exn
  for_ (moduleInterfaces mod0) $ \iface ->
    withContext (ContextDefInterface mod0 iface IPWhole) $
      checkInterface mod0 iface
