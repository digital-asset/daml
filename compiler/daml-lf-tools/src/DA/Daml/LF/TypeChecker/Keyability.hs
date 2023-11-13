-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | This module performs a partial check ensuring that the types used as
-- template keys do not contain 'ContractId's. The check is partial in that
-- it will only check the key type expression and any types mentioned in it
-- which are defined in the same module. For lack of a better name, we refer
-- to this as the "keyability" of a type.
-- Note that this check is performed after the serializability checks, so we
-- do not have to worry about unserializable types being reported as keyable by
-- this check.
module DA.Daml.LF.TypeChecker.Keyability
  ( checkModule
  ) where

import Control.Lens (matching, toListOf)
import Control.Monad.Extra
import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Optics (_PRSelfModule, dataConsType)
import DA.Daml.LF.TypeChecker.Env
import DA.Daml.LF.TypeChecker.Error
import Data.Foldable (for_)
import Data.HashMap.Strict qualified as HMS
import Data.HashSet qualified as HS
import Data.NameMap qualified as NM
import Data.Semigroup.FixedPoint (leastFixedPointBy)

newtype CurrentModule = CurrentModule  ModuleName
newtype UnkeyableTyConSet = UnkeyableTyConSet (HS.HashSet TypeConName)

-- | Check whether a module satisfies all "keyability" constraints.
checkModule :: MonadGamma m => Module -> m ()
checkModule mod0 = do
  let
    currentModule = CurrentModule (moduleName mod0)
    dataTypes = moduleDataTypes mod0
    unkeyableTyCons = findUnkeyableTyConsInModule currentModule dataTypes

  for_ (moduleTemplates mod0) $ \tpl ->
    for_ (tplKey tpl) $ \key ->
      withContext (ContextTemplate mod0 tpl TPKey) $ do
        checkKeyType currentModule unkeyableTyCons (tplKeyType key)

findUnkeyableTyConsInModule ::
     CurrentModule
  -> NM.NameMap DefDataType
  -> UnkeyableTyConSet
findUnkeyableTyConsInModule currentModule dataTypes = do
  let eqs =
        [ (dataTypeCon dataType, keyable, deps)
        | dataType <- NM.toList dataTypes
        , let (keyable, deps) =
                case keyabilityConditionsDataType currentModule dataType of
                  Nothing -> (False, [])
                  Just deps0 -> (True, HS.toList deps0)
        ]
  case leastFixedPointBy (&&) eqs of
    Left name -> error ("Reference to unknown data type: " ++ show name)
    Right keyabilities ->
      UnkeyableTyConSet $ HMS.keysSet $ HMS.filter not keyabilities

-- | Determine whether a data type preserves "keyability".
keyabilityConditionsDataType ::
     CurrentModule
  -> DefDataType
  -> Maybe (HS.HashSet TypeConName)
keyabilityConditionsDataType currentModule (DefDataType _loc _ _ _ cons) =
  mconcatMapM
    (keyabilityConditionsType currentModule)
    (toListOf dataConsType cons)

-- | Determine whether a type is "keyable".
--
-- Type constructors from the current module are returned as "keyability"
-- conditions, while type constructors from other modules are assumed to be "keyable"
keyabilityConditionsType ::
     CurrentModule
  -> Type
  -> Maybe (HS.HashSet TypeConName)
keyabilityConditionsType (CurrentModule currentModuleName) = go
  where
    unkeyable = Nothing
    noConditions = Just HS.empty
    conditionallyOn = Just . HS.singleton

    go = \case
      TContractId{} -> unkeyable

      TList typ -> go typ
      TOptional typ -> go typ
      TTextMap typ -> go typ
      TGenMap t1 t2 -> HS.union <$> go t1 <*> go t2
      TApp tfun targ -> HS.union <$> go tfun <*> go targ

      TCon qtcon
        | Right tconName <- matching (_PRSelfModule currentModuleName) qtcon ->
            conditionallyOn tconName
        | otherwise -> noConditions

      TNumeric{} -> noConditions
      TVar{} -> noConditions
      TBuiltin{} -> noConditions

      -- By this point, the only remaining type synonyms are the ones for constraints,
      -- which are deemed unserializable by DA.Daml.LF.TypeChecker.Serializability, so
      -- we don't need any special handling here.
      TSynApp{} -> noConditions

      -- These are also unserializable so no special handling is needed either.
      TNat{} -> noConditions
      TForall{} -> noConditions
      TStruct{} -> noConditions

-- | Check whether a type is "keyable".
checkKeyType :: MonadGamma m => CurrentModule -> UnkeyableTyConSet -> Type -> m ()
checkKeyType currentModule (UnkeyableTyConSet unkeyableTyCons) typ = do
  case keyabilityConditionsType currentModule typ of
    Just conds
      | HS.null (HS.intersection conds unkeyableTyCons) -> pure ()
    _ -> throwWithContext (EExpectedKeyTypeWithoutContractId typ)
