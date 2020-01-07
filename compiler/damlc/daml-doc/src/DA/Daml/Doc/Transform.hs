-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Transform
  ( DocOption(..)
  , DocOptions(..)
  , compileDocOptions
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
  | ExcludeInterfaces [String]
  | DataOnly            -- ^ do not generate doc.s for functions and classes
  | IgnoreAnnotations   -- ^ move or hide items based on annotations in the source
  | OmitEmpty           -- ^ omit all items that do not have documentation
  deriving (Eq, Ord, Show, Read)

getIncludeModules :: DocOption -> Maybe [String]
getIncludeModules = \case
    IncludeModules xs -> Just xs
    _ -> Nothing

getExcludeModules :: DocOption -> Maybe [String]
getExcludeModules = \case
    ExcludeModules xs -> Just xs
    _ -> Nothing

data DocOptions = DocOptions
    { doIncludeModules :: Maybe [String]
    , doExcludeModules :: Maybe [String]
    , doExcludeInterfaces :: Set.Set String
    , doDataOnly :: Bool -- ^ do not generate docs for functions and classes
    , doIgnoreAnnotations :: Bool -- ^ ignore MOVE and HIDE annotations
    , doOmitEmpty :: Bool -- ^ omit all items that do not have documentation
    }

doFilterModules :: DocOptions -> ModuleDoc -> Bool
doFilterModules DocOptions{..} m = includeModuleFilter && excludeModuleFilter
  where
    includeModuleFilter :: Bool
    includeModuleFilter = maybe True moduleMatchesAny doIncludeModules

    excludeModuleFilter :: Bool
    excludeModuleFilter = maybe True (not . moduleMatchesAny) doExcludeModules

    moduleMatchesAny :: [String] -> Bool
    moduleMatchesAny ps = any (?== name) (map withSlashes ps)

    withSlashes :: String -> String
    withSlashes = replace "." [pathSeparator]

    name :: String
    name = withSlashes . T.unpack . unModulename . md_name $ m

doFilterInstance :: DocOptions -> InstanceDoc -> Bool
doFilterInstance DocOptions{..} InstanceDoc{..} =
    let nameM = T.unpack . unTypename <$> getTypeAppName id_type
    in maybe True (not . (`Set.member` doExcludeInterfaces)) nameM

compileDocOptions :: [DocOption] -> DocOptions
compileDocOptions ds = DocOptions
    { doIncludeModules = foldMap getIncludeModules ds
    , doExcludeModules = foldMap getExcludeModules ds
    , doExcludeInterfaces = Set.empty
    , doDataOnly = DataOnly `elem` ds
    , doIgnoreAnnotations = IgnoreAnnotations `elem` ds
    , doOmitEmpty = OmitEmpty `elem` ds
    }

applyTransform :: [DocOption] -> [ModuleDoc] -> [ModuleDoc]
applyTransform = applyTransform' . compileDocOptions

applyTransform' :: DocOptions -> [ModuleDoc] -> [ModuleDoc]
applyTransform' opts@DocOptions{..}
    = distributeInstanceDocs opts
    . (if doOmitEmpty then mapMaybe dropEmptyDocs else id)
    . (if doIgnoreAnnotations then id else applyAnnotations)
    . (if doDataOnly then map pruneNonData else id)
    . filter (doFilterModules opts)
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
distributeInstanceDocs :: DocOptions -> [ModuleDoc] -> [ModuleDoc]
distributeInstanceDocs opts docs =
    let instanceMap = getInstanceMap docs
    in map (addInstances instanceMap) docs

  where

    getInstanceMap :: [ModuleDoc] -> InstanceMap
    getInstanceMap docs =
        Map.unionsWith Set.union (map getModuleInstanceMap docs)

    getModuleInstanceMap :: ModuleDoc -> InstanceMap
    getModuleInstanceMap ModuleDoc{..}
        = Map.unionsWith Set.union
        . map getInstanceInstanceMap
        . filter (doFilterInstance opts)
        $ md_instances

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
