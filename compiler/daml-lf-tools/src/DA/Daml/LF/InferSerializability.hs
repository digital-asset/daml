-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import DA.Daml.LF.TypeChecker.Serializability (serializabilityConditionsDataType)

inferModule :: World -> Version -> Module -> Either String Module
inferModule world0 version mod0 = do
  let modName = moduleName mod0
  let dataTypes = moduleDataTypes mod0
  let templates1 = NM.namesSet (moduleTemplates mod0)
  let eqs =
        [ (dataTypeCon dataType, serializable, deps)
        | dataType <- NM.toList dataTypes
        , let (serializable, deps) =
                case serializabilityConditionsDataType world0 version (Just (modName, templates1)) dataType of
                  Left _ -> (False, [])
                  Right deps0 -> (True, HS.toList deps0)
        ]
  case leastFixedPointBy (&&) eqs of
    Left name -> throwError ("Reference to unknown data type: " ++ show name)
    Right serializabilities -> do
      let updateDataType dataType =
            dataType{dataSerializable = IsSerializable (HMS.lookupDefault False (dataTypeCon dataType) serializabilities)}
      pure mod0{moduleDataTypes = NM.map updateDataType dataTypes}
