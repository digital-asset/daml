-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.InferSerializability
  ( SerializabilityOptions (..)
  , inferModule
  ) where

import           Control.Monad (when)
import           Control.Monad.Error.Class
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import qualified Data.NameMap as NM
import           Data.Semigroup.FixedPoint (leastFixedPointBy)
import qualified Data.Text as T

import DA.Daml.LF.Ast
import DA.Daml.LF.TypeChecker.Serializability (CurrentModule(..), serializabilityConditionsDataType)

data SerializabilityOptions = SerializabilityOptions
  { soForceUtilityPackage  :: Bool
  , soExplicitSerializable :: Bool
  } deriving (Show)

inferModule :: World -> SerializabilityOptions -> Module -> Either String Module
inferModule world0 SerializabilityOptions {..} mod0 =
  case moduleName mod0 of
    -- If ExplicitSerializable is on, we want to do nothing at all.
    -- LFConversion will have already added the IsSerializable annotation where
    -- an instance exists.  Then TypeChecker/Serializability will check
    -- consistency of these instances later.
    _ | soExplicitSerializable -> do
      pure mod0
    -- Unstable parts of stdlib mustn't contain serializable types, because if they are 
    -- serializable, then the upgrading checks run on the datatypes and this causes problems. 
    -- Therefore, we mark the datatypes as not-serializable, so that upgrades checks don't trigger.
    -- For more information on this issue, refer to issue 
    -- https://github.com/digital-asset/daml/issues/19338
    --
    -- Note that we will no longer need this hack when ExplicitSerializable is on.
    ModuleName ["GHC", "Stack", "Types"] -> pure mod0
    ModuleName ["DA", "Numeric"] -> pure mod0
    _ | soForceUtilityPackage -> do
      let mkErr name =
            throwError $ T.unpack $ "No " <> name <> " definitions permitted in forced utility packages (Module " <> moduleNameString (moduleName mod0) <> ")"
      when (not $ NM.null $ moduleTemplates mod0) $ mkErr "template"
      when (not $ NM.null $ moduleInterfaces mod0) $ mkErr "interface"
      when (not $ NM.null $ moduleExceptions mod0) $ mkErr "exception"
      pure mod0
    -- If ExplicitSerializable is not on, we do some inference to decide on
    -- which types to put the serializability annotations.
    --
    -- This happens by analysing each type individually (while keeping a list
    -- of its dependencies), and then propagating this dependency information
    -- using leastFixedPointBy.
    _ -> do
      let modName = moduleName mod0
      let dataTypes = moduleDataTypes mod0
      let interfaces = NM.namesSet (moduleInterfaces mod0)
      let eqs =
            [ (dataTypeCon dataType, serializable, deps)
            | dataType <- NM.toList dataTypes
            , let (serializable, deps) =
                    case serializabilityConditionsDataType world0 (Just $ CurrentModule modName interfaces) dataType of
                      Left _ -> (False, [])
                      Right deps0 -> (True, HS.toList deps0)
            ]
      case leastFixedPointBy (&&) eqs of
        Left name -> throwError ("Reference to unknown data type: " ++ show name)
        Right serializabilities -> do
          let updateDataType dataType =
                dataType{dataSerializable = IsSerializable (HMS.lookupDefault False (dataTypeCon dataType) serializabilities)}
          pure mod0{moduleDataTypes = NM.map updateDataType dataTypes}
