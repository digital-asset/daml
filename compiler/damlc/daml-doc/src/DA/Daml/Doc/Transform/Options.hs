-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Transform.Options
    ( TransformOptions (..)
    , defaultTransformOptions
    , keepModule
    , keepInstance
    ) where

import DA.Daml.Doc.Types

import Data.List.Extra (replace)
import System.FilePath (pathSeparator) -- because FilePattern uses it
import System.FilePattern ((?==))
import qualified Data.Set as Set
import qualified Data.Text as T


data TransformOptions = TransformOptions
    { to_includeModules :: Maybe [String]
    , to_excludeModules :: Maybe [String]
    , to_excludeInstances :: Set.Set String
    , to_dropOrphanInstances :: Bool -- ^ remove orphan instance docs
    , to_dataOnly :: Bool -- ^ do not generate docs for functions and classes
    , to_ignoreAnnotations :: Bool -- ^ ignore MOVE and HIDE annotations
    , to_omitEmpty :: Bool -- ^ omit all items that do not have documentation
    }

defaultTransformOptions :: TransformOptions
defaultTransformOptions = TransformOptions
    { to_includeModules = Nothing
    , to_excludeModules = Nothing
    , to_excludeInstances = Set.empty
    , to_dropOrphanInstances = False
    , to_dataOnly = False
    , to_ignoreAnnotations = False
    , to_omitEmpty = False
    }

keepModule :: TransformOptions -> ModuleDoc -> Bool
keepModule TransformOptions{..} m = includeModuleFilter && excludeModuleFilter
  where
    includeModuleFilter :: Bool
    includeModuleFilter = maybe True moduleMatchesAny to_includeModules

    excludeModuleFilter :: Bool
    excludeModuleFilter = maybe True (not . moduleMatchesAny) to_excludeModules

    moduleMatchesAny :: [String] -> Bool
    moduleMatchesAny = any ((?== name) . withSlashes)

    withSlashes :: String -> String
    withSlashes = replace "." [pathSeparator]

    name :: String
    name = withSlashes . T.unpack . unModulename . md_name $ m

keepInstance :: TransformOptions -> InstanceDoc -> Bool
keepInstance TransformOptions{..} InstanceDoc{..} =
    let nameM = T.unpack . unTypename <$> getTypeAppName id_type
    in maybe True (`Set.notMember` to_excludeInstances) nameM

