-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.InferSerializability
  ( inferModule
  , inferPackage
  ) where

import           Control.Monad.Error.Class
import           Data.Foldable (foldlM)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import qualified Data.NameMap as NM
import           Data.Semigroup.FixedPoint (leastFixedPointBy)

import DA.Daml.LF.Ast
import DA.Daml.LF.TypeChecker.Serializability (serializabilityConditionsDataType)

inferModule :: World -> Module -> Either String Module
inferModule world0 mod0 = do
  let modName = moduleName mod0
  let dataTypes = moduleDataTypes mod0
  let templates1 = NM.namesSet (moduleTemplates mod0)
  let eqs =
        [ (dataTypeCon dataType, serializable, deps)
        | dataType <- NM.toList dataTypes
        , let (serializable, deps) =
                case serializabilityConditionsDataType world0 (Just (modName, templates1)) dataType of
                  Left _ -> (False, [])
                  Right deps0 -> (True, HS.toList deps0)
        ]
  case leastFixedPointBy (&&) eqs of
    Left name -> throwError ("Reference to unknown data type: " ++ show name)
    Right serializabilities -> do
      let updateDataType dataType =
            dataType{dataSerializable = IsSerializable (HMS.lookupDefault False (dataTypeCon dataType) serializabilities)}
      pure mod0{moduleDataTypes = NM.map updateDataType dataTypes}

-- | Infer the serializability information for a package. Assumes the modules
-- have been sorted topologically. Does not change the order of the modules.
-- See 'DA.Daml.LF.Ast.Util.topoSortPackage'.
inferPackage :: [(PackageId, Package)] -> Package -> Either String Package
inferPackage pkgDeps (Package version mods0) = do
      let infer1 (mods1, world0) mod0 = do
            mod1 <- inferModule world0 mod0
            pure (NM.insert mod1 mods1, extendWorldSelf mod1 world0)
      Package version . fst <$> foldlM infer1 (NM.empty, initWorld pkgDeps version) mods0
