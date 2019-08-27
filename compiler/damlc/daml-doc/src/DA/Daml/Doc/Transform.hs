-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Transform
  ( DocOption(..)
  , applyTransform
  ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Annotate

import Data.Maybe
import Data.List.Extra
import System.FilePath (pathSeparator) -- because FilePattern uses it
import System.FilePattern
import qualified Data.Text as T
import qualified Data.Map as Map
import qualified Data.Set as Set

-- | Documentation filtering options, applied in the order given here
data DocOption =
  IncludeModules [String]    -- ^ only include modules whose name matches one of the given file patterns
  | ExcludeModules [String]  -- ^ exclude modules whose name matches one of the given file patterns
  | DataOnly            -- ^ do not generate doc.s for functions and classes
  | IgnoreAnnotations   -- ^ move or hide items based on annotations in the source
  | OmitEmpty           -- ^ omit all items that do not have documentation
  deriving (Eq, Ord, Show, Read)


applyTransform :: [DocOption] -> [ModuleDoc] -> [ModuleDoc]
applyTransform opts = distributeInstanceDocs . maybeDoAnnotations opts'
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
      where name = withSlashes . T.unpack . unModulename . md_name $ m

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
          isNothing cl_descr && all isEmpty cl_methods

instance IsEmpty ClassMethodDoc where
    isEmpty ClassMethodDoc{..} = isNothing cm_descr

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

type InstanceMap = Map.Map Anchor (Set.Set InstanceDoc)

-- | Add relevant instances to every type and class.
distributeInstanceDocs :: [ModuleDoc] -> [ModuleDoc]
distributeInstanceDocs docs =
    let instanceMap = getInstanceMap docs
    in map (addInstances instanceMap) docs

  where

    getInstanceMap :: [ModuleDoc] -> InstanceMap
    getInstanceMap docs =
        Map.unionsWith Set.union (map getModuleInstanceMap docs)

    getModuleInstanceMap :: ModuleDoc -> InstanceMap
    getModuleInstanceMap ModuleDoc{..} =
        Map.unionsWith Set.union (map getInstanceInstanceMap md_instances)

    getInstanceInstanceMap :: InstanceDoc -> InstanceMap
    getInstanceInstanceMap inst = Map.fromList
        [ (anchor, Set.singleton inst)
        | anchor <- Set.toList . getTypeAnchors $ id_type inst ]

    -- | Get the set of internal references i.e. anchors in the type expression.
    getTypeAnchors :: Type -> Set.Set Anchor
    getTypeAnchors = \case
        TypeApp (Just (Reference Nothing anchor)) _ args -> Set.unions
            $ Set.singleton anchor
            : map getTypeAnchors args
        TypeApp _ _ args -> Set.unions $ map getTypeAnchors args
        TypeFun parts -> Set.unions $ map getTypeAnchors parts
        TypeTuple parts -> Set.unions $ map getTypeAnchors parts
        TypeList p -> getTypeAnchors p
        TypeLit _ -> Set.empty

    addInstances :: InstanceMap -> ModuleDoc -> ModuleDoc
    addInstances imap ModuleDoc{..} = ModuleDoc
        { md_name = md_name
        , md_anchor = md_anchor
        , md_descr = md_descr
        , md_functions = md_functions
        , md_templates = md_templates
        , md_templateInstances = md_templateInstances
        , md_classes = map (addClassInstances imap) md_classes
        , md_adts = map (addTypeInstances imap) md_adts
        , md_instances = md_instances
        }

    addClassInstances :: InstanceMap -> ClassDoc -> ClassDoc
    addClassInstances imap cl = cl
        { cl_instances = Set.toList <$> do
            anchor <- cl_anchor cl
            Map.lookup anchor imap
        }

    addTypeInstances :: InstanceMap -> ADTDoc -> ADTDoc
    addTypeInstances imap ad = ad
        { ad_instances = Set.toList <$> do
            anchor <- ad_anchor ad
            Map.lookup anchor imap
        }
