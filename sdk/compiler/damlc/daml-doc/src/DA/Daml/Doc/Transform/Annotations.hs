-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Transform.Annotations
    ( applyAnnotations
    ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Anchor

import qualified Data.Text as T
import Data.List.Extra
import Data.Either
import Data.Maybe (fromMaybe, isNothing, maybeToList)
import Control.Applicative ((<|>))

-- | Apply HIDE and MOVE annotations.
applyAnnotations :: [ModuleDoc] -> [ModuleDoc]
applyAnnotations = applyMove . applyHide

-- | Apply the MOVE annotation, which moves all the docs from one
-- module to another.
applyMove :: [ModuleDoc] -> [ModuleDoc]
applyMove
    = map (foldr1 combineModules)
    . groupSortOn md_name
    . concatMap performRenames
  where
    defModule :: Modulename -> ModuleDoc
    defModule name = ModuleDoc
      { md_name = name
      , md_anchor = Just (moduleAnchor name)
      , md_descr = Nothing
      , md_warn = Nothing
      , md_templates = []
      , md_interfaces = []
      , md_adts = []
      , md_functions = []
      , md_classes = []
      , md_instances = []
      }

    isEmptyModule :: ModuleDoc -> Bool
    isEmptyModule ModuleDoc{..} =
      null md_templates
        && null md_interfaces
        && null md_adts
        && null md_functions
        && null md_classes
        && null md_instances
        && isNothing md_descr

    performRenames :: ModuleDoc -> [ModuleDoc]
    performRenames md@ModuleDoc{..} =
      let md' = renameModule md
          isMoveComponent :: (a -> [DocText]) -> (a -> [DocText] -> a) -> a -> Either (Modulename, a) a
          isMoveComponent getDesc setDesc c
            = fromMaybe (Right c) $ do
                (newMod, newDesc) <- isMove $ getDesc c
                pure $ Left (newMod, setDesc c newDesc)
          moveComponents :: [a] -> (a -> [DocText]) -> (a -> [DocText] -> a) -> (ModuleDoc -> [a] -> ModuleDoc) -> ([ModuleDoc], [a])
          moveComponents cs getDesc setDesc writeComponents =
            let (movedComponents, stillComponents) = partitionEithers $ map (isMoveComponent getDesc setDesc) cs
                newMods = flip map movedComponents $ \(new, c) -> writeComponents (defModule new) [c]
             in (newMods, stillComponents)
          (newModsTemplates, templates) = moveComponents md_templates td_descr (\td d -> td {td_descr = d}) (\md ts -> md {md_templates = ts})
          (newModsInterfaces, interfaces) = moveComponents md_interfaces if_descr (\id d -> id {if_descr = d}) (\md is -> md {md_interfaces = is})
          (newModsAdts, adts) = moveComponents md_adts ad_descr (\adr d -> adr {ad_descr = d}) (\md adts -> md {md_adts = adts})
          (newModsFunctions, functions) = moveComponents md_functions fct_descr (\fd d -> fd {fct_descr = d}) (\md fs -> md {md_functions = fs})
          (newModsClasses, classes) = moveComponents md_classes cl_descr (\cld d -> cld {cl_descr = d}) (\md cs -> md {md_classes = cs})
          (newModsInstances, instances) = moveComponents md_instances id_descr (\iid d -> iid {id_descr = d}) (\md is -> md {md_instances = is})
          filteredMd = md'
            { md_templates = templates
            , md_interfaces = interfaces
            , md_adts = adts
            , md_functions = functions
            , md_classes = classes
            , md_instances = instances
            }
          -- If moving docs made the module empty, we can omit it. Cannot otherwise.
          lFilteredMd = ([filteredMd | not (isEmptyModule filteredMd && not (isEmptyModule md'))])
       in lFilteredMd ++ newModsTemplates ++ newModsInterfaces ++ newModsAdts ++ newModsFunctions ++ newModsClasses ++ newModsInstances

    -- | Rename module according to its MOVE annotation, if present.
    -- If the module is renamed, we drop the rest of the module's
    -- description.
    renameModule :: ModuleDoc -> ModuleDoc
    renameModule md@ModuleDoc{..}
        | Just (new, _) <- isMove (maybeToList md_descr) = md
            { md_name = new
            , md_anchor = Just (moduleAnchor new)
                -- Update the module anchor
            , md_descr = Nothing
                -- Drop the renamed module's description.
            }
        | otherwise = md

    -- | Combine two modules with the same name.
    combineModules :: ModuleDoc -> ModuleDoc -> ModuleDoc
    combineModules m1 m2 = ModuleDoc
        { md_anchor = md_anchor m1
        , md_name = md_name m1
        , md_descr = md_descr m1 <|> md_descr m2
            -- The renamed module's description was dropped,
            -- so in this line we always prefers the original
            -- module description.
        , md_warn = md_warn m1 <|> md_warn m2
            -- Same as for md_descr
        , md_adts = md_adts m1 ++ md_adts m2
        , md_functions = md_functions m1 ++ md_functions m2
        , md_templates = md_templates m1 ++ md_templates m2
        , md_interfaces = md_interfaces m1 ++ md_interfaces m2
        , md_classes = md_classes m1 ++ md_classes m2
        , md_instances = md_instances m1 ++ md_instances m2
        }

-- | Apply the HIDE annotation, which removes the current subtree from
-- the docs. This can be applied to an entire module, or to a specific
-- type, a constructor, a field, a class, a method, or a function.
applyHide :: [ModuleDoc] -> [ModuleDoc]
applyHide = concatMap onModule
    where
        onModule md@ModuleDoc{..}
            | isHide (maybeToList md_descr) = []
            | otherwise = pure md
                    {md_templates = concatMap onTemplate md_templates
                    ,md_adts = concatMap onADT md_adts
                    ,md_functions = concatMap onFunction md_functions
                    ,md_classes = concatMap onClass md_classes
                    }

        -- be careful, we don't support hiding arbitrary bits within a data type or template
        -- as that would be a security risk
        onTemplate x = [x | not $ isHide $ td_descr x]
        onFunction x = [x | not $ isHide $ fct_descr x]
        onMethod x = [x | not $ isHide $ cm_descr x]
        onClass x
            | isHide $ cl_descr x = []
            | ClassDoc {..} <- x = [x { cl_methods = concatMap onMethod cl_methods }]

        onADT x
            | isHide $ ad_descr x = []
            | ADTDoc{..} <- x, all (isHide . ac_descr) ad_constrs = pure x{ad_constrs = []}
            | otherwise = [x]

isHide :: [DocText] -> Bool
isHide x = fromMaybe False $ do
  firstDoc <- unDocText . fst <$> uncons x
  firstLine <- fst <$> uncons firstDoc
  pure $ ["HIDE"] `isPrefixOf` T.words firstLine

isMove :: [DocText] -> Maybe (Modulename, [DocText])
isMove docs = do
  (firstDoc, restDocs) <- uncons docs
  (firstLine, restLines) <- uncons $ unDocText firstDoc
  case T.words firstLine of
    ("MOVE":modName:restLine) ->
      let firstDocLines = [T.unwords restLine | not $ null restLine] ++ restLines
       in Just (Modulename modName, [DocText firstDocLines | not $ null firstDocLines] ++ restDocs)
    _ -> Nothing
