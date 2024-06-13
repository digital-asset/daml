-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.InferSerializability
  ( inferModule
  ) where

import           Control.Monad.Error.Class
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import qualified Data.NameMap as NM
import           Data.Semigroup.FixedPoint (leastFixedPointBy)

import DA.Daml.LF.Ast
import DA.Daml.LF.TypeChecker.Serializability (CurrentModule(..), serializabilityConditionsDataType)

inferModule :: World -> Version -> Module -> Either String Module
inferModule world0 version mod0 =
  case moduleName mod0 of
    -- Unstable parts of stdlib mustn't contain serializable types, because if they are 
    -- serializable, then the upgrading checks run on the datatypes and this causes problems. 
    -- Therefore, we mark the datatypes as not-serializable, so that upgrades checks don't trigger.
    -- For more information on this issue, refer to issue 
    -- https://github.com/digital-asset/daml/issues/19338issues/19338
    ModuleName ["GHC", "Stack", "Types"] -> pure mod0
    _                      -> do
      let modName = moduleName mod0
      let dataTypes = moduleDataTypes mod0
      let interfaces = NM.namesSet (moduleInterfaces mod0)
      let eqs =
            [ (dataTypeCon dataType, serializable, deps)
            | dataType <- NM.toList dataTypes
            , let (serializable, deps) =
                    case serializabilityConditionsDataType world0 version (Just $ CurrentModule modName interfaces) dataType of
                      Left _ -> (False, [])
                      Right deps0 -> (True, HS.toList deps0)
            ]
      case leastFixedPointBy (&&) eqs of
        Left name -> throwError ("Reference to unknown data type: " ++ show name)
        Right serializabilities -> do
          let updateDataType dataType =
                dataType{dataSerializable = IsSerializable (HMS.lookupDefault False (dataTypeCon dataType) serializabilities)}
          pure mod0{moduleDataTypes = NM.map updateDataType dataTypes}
