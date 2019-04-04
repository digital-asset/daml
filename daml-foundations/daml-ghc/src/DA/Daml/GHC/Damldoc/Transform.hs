-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.GHC.Damldoc.Transform
  ( DocOption(..)
  , applyTransform
  ) where

import DA.Daml.GHC.Damldoc.Types
import DA.Daml.GHC.Damldoc.Annotate

import Data.Maybe
import Data.List.Extra
import System.FilePath (pathSeparator) -- because FilePattern uses it
import System.FilePattern
import qualified Data.Text as T

-- | Documentation filtering options, applied in the order given here
data DocOption =
  IncludeModules [String]    -- ^ only include modules whose name matches one of the given file patterns
  | ExcludeModules [String]  -- ^ exclude modules whose name matches one of the given file patterns
  | DataOnly            -- ^ do not generate doc.s for functions and classes
  | IgnoreAnnotations   -- ^ move or hide items based on annotations in the source
  | OmitEmpty           -- ^ omit all items that do not have documentation
  deriving (Eq, Ord, Show, Read)


applyTransform :: [DocOption] -> [ModuleDoc] -> [ModuleDoc]
applyTransform opts docs = maybeDoAnnotations opts' docs
  where
    opts' = nubOrd $ sort opts

    -- Options are processed in order, the option list is assumed to be sorted
    -- (without duplicates).
    processWith :: [DocOption] -> [ModuleDoc] -> [ModuleDoc]

    processWith [] ms = ms

    -- merge adjacent file pattern lists
    processWith (IncludeModules rs : IncludeModules rs' : rest) ms
      = processWith (IncludeModules (rs <> rs') : rest) ms
    processWith (ExcludeModules rs : ExcludeModules rs' : rest) ms
      = processWith (ExcludeModules (rs <> rs') : rest) ms

    processWith (IncludeModules rs : rest) ms
      = maybeDoAnnotations rest $ filter (moduleMatchesAny $ map withSlashes rs) ms
    processWith (ExcludeModules rs : rest) ms
      = maybeDoAnnotations rest $ filter (not . moduleMatchesAny (map withSlashes rs)) ms

    processWith (DataOnly : rest) ms = maybeDoAnnotations rest $ map prune ms

    processWith (IgnoreAnnotations : rest) ms = processWith rest ms

    -- Empty items are (recursively) dropped after applying MOVE and HIDE.
    -- If we reach OmitEmpty, it must be the single last option in the sorted list.
    -- Guaranteed by the call with nubOrd (sort opts) above
    processWith [OmitEmpty] ms       = mapMaybe dropEmptyDocs ms
    processWith (OmitEmpty : rest) _ = error $ "Remainder options after OmitEmpty: "
                                             <> show rest


    -- Apply annotations after DataOnly to save work, unless IgnoreAnnotations
    -- is present. Assuming sorted and deduplicated options, the
    -- IgnoreAnnotations option must come second-last.
    maybeDoAnnotations [] ms = applyAnnotations ms
    maybeDoAnnotations things@(opt : rest) ms =
      case compare opt IgnoreAnnotations of
        LT -> processWith things ms  -- not there yet, continue processing
        EQ -> processWith rest ms    -- found IgnoreAnnotations, so proceed
                                     -- without applying them
        GT -> processWith things $ applyAnnotations ms
          -- went past it without finding IgnoreAnnotations, so apply them


    prune :: ModuleDoc -> ModuleDoc
    prune m = m{ md_functions = [], md_classes = [] }

    -- conversions to use file pattern matcher

    moduleMatchesAny :: [FilePattern] -> ModuleDoc -> Bool
    moduleMatchesAny ps m = any (?== name) ps
      where name = withSlashes $ T.unpack $ md_name m

    withSlashes :: String -> String
    withSlashes = replace "." [pathSeparator]


-- emptiness of documentation, recursing into items
dropEmptyDocs :: ModuleDoc -> Maybe ModuleDoc
dropEmptyDocs m
  | isEmpty m = Nothing
  | otherwise = Just m

-- Helper class for emptiness test
class IsEmpty a
  where isEmpty :: a -> Bool

instance IsEmpty ModuleDoc
  where isEmpty ModuleDoc{..} =
          all isEmpty md_templates
          && all isEmpty md_adts
          && all isEmpty md_functions
          && all isEmpty md_classes
          -- If the module description is the only documentation item, the
          -- doc.s aren't very useful.
          && (isNothing md_descr
               || null md_adts && null md_templates
                  && null md_functions && null md_classes)

instance IsEmpty TemplateDoc
  where isEmpty TemplateDoc{..} =
          isNothing td_descr
          && all isEmpty td_payload
          && all isEmpty td_choices

instance IsEmpty ChoiceDoc
  where isEmpty ChoiceDoc{..} =
          isNothing cd_descr
          && all isEmpty cd_fields

instance IsEmpty ClassDoc
  where isEmpty ClassDoc{..} =
          isNothing cl_descr && all isEmpty cl_functions

instance IsEmpty ADTDoc
  where isEmpty ADTDoc{..} =
          isNothing ad_descr && all isEmpty ad_constrs
        isEmpty TypeSynDoc{..} =
          isNothing ad_descr

instance IsEmpty ADTConstr
  where isEmpty PrefixC{..} =
          isNothing ac_descr
        isEmpty RecordC{..} =
          isNothing ac_descr && all isEmpty ac_fields

instance IsEmpty FieldDoc
  where isEmpty FieldDoc{..} = isNothing fd_descr

instance IsEmpty FunctionDoc
  where isEmpty FunctionDoc{..} = isNothing fct_descr
