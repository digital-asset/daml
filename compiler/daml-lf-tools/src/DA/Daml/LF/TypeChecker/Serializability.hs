-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | This module provides functions to perform the DAML-LF constraint checks on
-- types in certain positions. In fact, we also need to do some form of
-- "constraint inference". To perform this inference in an incremental fashion,
-- we also provide a way to augment a 'ModuleInterface' with constraint
-- information about the exported data types in 'augmentInterface'.
--
-- Checking whether a function or template definition complies with the DAML-LF
-- type constraints is straightforward. It is implemented in 'checkModule', which
-- assumes that the constraint information on the data types in the module being
-- checked have already been inferred. In other words, the environment must
-- contain the 'ModuleInterface' produced by 'augmentInterface'.
module DA.Daml.LF.TypeChecker.Serializability
  ( serializabilityConditionsDataType
  , checkModule
  ) where

import DA.Prelude

import           Control.Lens (matching, toListOf, view)
import           Control.Monad.Extra (mconcatMapM)
import           Data.Either (isRight)
import           Data.Foldable (for_)
import qualified Data.HashSet as HS

import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Optics (_PRSelfModule, dataConsType)
import DA.Daml.LF.TypeChecker.Env
import DA.Daml.LF.TypeChecker.Error

-- | Determine whether a type is serializable. When a module name is given,
-- data types in this module are returned rather than lookup up in the world.
-- If no module name is given, the returned set is always empty.
serializabilityConditionsType
  :: World
  -> Maybe (ModuleName, HS.HashSet TypeConName)
     -- ^ References to data types in this module are returned rather than
     -- chased. They are considered to have an associated template exactly when
     -- they are contained in the hashset.
  -> HS.HashSet TypeVarName
     -- ^ Type variables that are bound by a surrounding data type definition
     -- if any. The check that all of them are of kind '*' must be performed by
     -- the caller.
  -> Type
  -> Either UnserializabilityReason (HS.HashSet TypeConName)
serializabilityConditionsType world0 mbModNameTpls vars = go
  where
    noConditions = Right HS.empty
    go = \case
      -- This is the only way 'ContractId's are allowed. Other cases handled below.
      TContractId typ@(TCon tcon) | isTemplate -> go typ
        where
          isTemplate
            | Just (modName, tpls) <- mbModNameTpls
            , Right tconName <- matching (_PRSelfModule modName) tcon =
                tconName `HS.member` tpls
            | otherwise = isRight $ lookupTemplate tcon world0
      -- This is the only way 'List's and 'Optional's are allowed. Other cases handled below.
      TList typ -> go typ
      TOptional typ -> go typ
      TMap typ -> go typ
      TVar v
        | v `HS.member` vars -> noConditions
        | otherwise -> Left (URFreeVar v)
      TCon tcon
        | Just (modName, _) <- mbModNameTpls
        , Right tconName <- matching (_PRSelfModule modName) tcon ->
            Right (HS.singleton tconName)
        | isSerializable tcon -> noConditions
        | otherwise -> Left (URDataType tcon)
        where
          isSerializable tconRef =
            either
              (error . showString "Serializablity.checkModule: " . show)
              (getIsSerializable . dataSerializable)
              (lookupDataType tconRef world0)
      TApp tfun targ -> HS.union <$> go tfun <*> go targ
      TBuiltin builtin -> case builtin of
        BTInt64 -> noConditions
        BTDecimal -> noConditions
        BTText -> noConditions
        BTTimestamp -> noConditions
        BTDate -> noConditions
        BTParty -> noConditions
        BTEnum _ -> noConditions
        BTList -> Left URList  -- 'List' is used as a higher-kinded type constructor.
        BTOptional -> Left UROptional  -- 'Optional' is used as a higher-kinded type constructor.
        BTMap -> Left URMap  -- 'Map' is used as a higher-kinded type constructor.
        BTUpdate -> Left URUpdate
        BTScenario -> Left URScenario
        BTContractId -> Left URContractId  -- 'ContractId' is used polymorphically or
                                           -- as a higher-kinded type constructor.
        BTArrow -> Left URFunction
      TForall{} -> Left URForall
      TTuple{} -> Left URTuple

-- | Determine whether a data type preserves serializability. When a module
-- name is given, -- data types in this module are returned rather than lookup
-- up in the world. If no module name is given, the returned set is always empty.
serializabilityConditionsDataType
  :: World
  -> Maybe (ModuleName, HS.HashSet TypeConName)
     -- ^ References to data types in this module are returned rather than
     -- chased. They are considered to have an associated template exactly when
     -- they are contained in the hashset.
  -> DefDataType
  -> Either UnserializabilityReason (HS.HashSet TypeConName)
serializabilityConditionsDataType world0 mbModNameTpls (DefDataType _loc _ _ params cons) =
  case find (\(_, k) -> k /= KStar) params of
    Just (v, k) -> Left (URHigherKinded v k)
    Nothing
      | DataVariant [] <- cons -> Left URUninhabitatedType
      | otherwise -> do
          let vars = HS.fromList (map fst params)
          mconcatMapM (serializabilityConditionsType world0 mbModNameTpls vars) (toListOf dataConsType cons)

-- | Check whether a type is serializable.
checkType :: MonadGamma m => SerializabilityRequirement -> Type -> m ()
checkType req typ = do
  world0 <- view world
  case serializabilityConditionsType world0 Nothing HS.empty typ of
    Left reason -> throwWithContext (EExpectedSerializableType req typ reason)
    Right _ -> pure ()

-- | Check whether a data type definition satisfies all serializability constraints.
checkDataType :: MonadGamma m => ModuleName -> DefDataType -> m ()
checkDataType modName dataType =
  when (getIsSerializable (dataSerializable dataType)) $ do
    world0 <- view world
    case serializabilityConditionsDataType world0 Nothing dataType of
      Left reason -> do
        let typ = TCon (Qualified PRSelf modName (dataTypeCon dataType))
        throwWithContext (EExpectedSerializableType SRDataType typ reason)
      Right _ -> pure ()

-- | Check whether a template satisfies all serializability constraints.
checkTemplate :: MonadGamma m => Module -> Template -> m ()
checkTemplate mod0 tpl = do
  let tcon = Qualified PRSelf (moduleName mod0) (tplTypeCon tpl)
  checkType SRTemplateArg (TCon tcon)
  for_ (tplChoices tpl) $ \ch -> withContext (ContextTemplate mod0 tpl $ TPChoice ch) $ do
    checkType SRChoiceArg (snd (chcArgBinder ch))
    checkType SRChoiceRes (chcReturnType ch)
  for_ (tplKey tpl) $ \key -> withContext (ContextTemplate mod0 tpl TPKey) $ do
    checkType SRKey (tplKeyType key)

-- | Check whether a module satisfies all serializability constraints.
checkModule :: MonadGamma m => Module -> m ()
checkModule mod0 = do
  for_ (moduleDataTypes mod0) $ \dataType ->
    withContext (ContextDefDataType mod0 dataType) $
      checkDataType (moduleName mod0) dataType
  for_ (moduleTemplates mod0) $ \tpl ->
    withContext (ContextTemplate mod0 tpl TPWhole) $
      checkTemplate mod0 tpl
