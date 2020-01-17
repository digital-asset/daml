-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Transform
  ( TransformOptions(..)
  , defaultTransformOptions
  , applyTransform
  ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Transform.Options
import DA.Daml.Doc.Transform.Annotations
import DA.Daml.Doc.Transform.Instances
import DA.Daml.Doc.Transform.DropEmpty

import Data.Maybe

applyTransform :: TransformOptions -> [ModuleDoc] -> [ModuleDoc]
applyTransform opts@TransformOptions{..}
    = (if to_dropOrphanInstances then map pruneOrphanInstances else id)
    . distributeInstanceDocs opts
    . (if to_omitEmpty then mapMaybe dropEmptyDocs else id)
    . (if to_ignoreAnnotations then id else applyAnnotations)
    . (if to_dataOnly then map pruneNonData else id)
    . filter (keepModule opts)
  where
    -- When --data-only is chosen, remove all non-data documentation. This
    -- includes functions, classes, and instances of all data types (but not
    -- template instances).
    pruneNonData :: ModuleDoc -> ModuleDoc
    pruneNonData m = m{ md_functions = []
                      , md_classes = []
                      , md_instances = []
                      , md_adts = map noInstances $ md_adts m
                      }

    noInstances :: ADTDoc -> ADTDoc
    noInstances d = d{ ad_instances = Nothing }

    pruneOrphanInstances :: ModuleDoc -> ModuleDoc
    pruneOrphanInstances m = m { md_instances = [] }
