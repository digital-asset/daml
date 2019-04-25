-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.GHC.Damldoc.HaddockParse(mkDocs) where

import           DA.Daml.GHC.Damldoc.Types                 as DDoc
import Development.IDE.Functions.Compile (CompileOpts(..))
import qualified Development.IDE.State.Service     as Service
import qualified Development.IDE.State.Rules     as Service
import           Development.IDE.Types.Diagnostics
import qualified Development.IDE.Logger as Logger


import           "ghc-lib" GHC
import qualified "ghc-lib-parser" Outputable                      as Out
import qualified "ghc-lib-parser" DynFlags                        as DF
import           "ghc-lib-parser" Bag (bagToList)

import           Control.Monad.Except.Extended             as Ex
import           Data.Char (isSpace)
import Data.Either.Extra
import           Data.List.Extra
import           Data.Maybe
import qualified Data.Map.Strict as MS
import qualified Data.Set as Set
import qualified Data.Text                                 as T
import           Data.Tuple.Extra                          (second)

-- | Parse, and process documentation in, a dependency graph of modules.
mkDocs :: CompileOpts ->
          [FilePath] ->
          Ex.ExceptT DiagnosticStore IO [ModuleDoc]
mkDocs opts fp = do
  parsed <- haddockParse opts fp
  pure $ map mkModuleDocs parsed

  where
    modDoc :: ParsedModule -> Maybe HsDocString
    modDoc = fmap unLoc . hsmodHaddockModHeader . unLoc . pm_parsed_source

    mkModuleDocs :: ParsedModule -> ModuleDoc
    mkModuleDocs m =
      let pairs   = pairDeclDocs m
          typeMap = MS.fromList $ mapMaybe getTypeDocs pairs
          (tmpls, choiceMap) = getTemplateData m
          fctDocs = mapMaybe getFctDocs pairs
          clsDocs = mapMaybe getClsDocs pairs
          tmplDocs = getTemplateDocs typeMap tmpls choiceMap
          allChoices = Set.unions $ MS.elems choiceMap
          adtDocs = MS.elems (typeMap `MS.withoutKeys` tmpls `MS.withoutKeys` allChoices)
      in ModuleDoc
           { md_name = T.pack . moduleNameString . moduleName . ms_mod . pm_mod_summary $ m
           , md_descr = fmap docToText (modDoc m)
           , md_adts = adtDocs
           , md_templates = tmplDocs
           , md_functions = fctDocs
           , md_classes = clsDocs
           }

-- | Return the names for a given signature.
-- Equivalent of Haddock’s Haddock.GhcUtils.sigNameNoLoc.
sigNameNoLoc :: Sig name -> [IdP name]
sigNameNoLoc (TypeSig    _   ns _)         = map unLoc ns
sigNameNoLoc (ClassOpSig _ _ ns _)         = map unLoc ns
sigNameNoLoc (PatSynSig  _   ns _)         = map unLoc ns
sigNameNoLoc (SpecSig    _   n _ _)        = [unLoc n]
sigNameNoLoc (InlineSig  _   n _)          = [unLoc n]
sigNameNoLoc (FixSig _ (FixitySig _ ns _)) = map unLoc ns
sigNameNoLoc _                             = []

-- | Given a class, return a map from member names to associated haddocks.
memberDocs :: TyClDecl GhcPs -> MS.Map (IdP GhcPs) [T.Text]
memberDocs cls = MS.fromList
  [ (name, doc) | (d, doc) <- subDecls, name <- declNames d]
  where
    declNames :: HsDecl GhcPs -> [IdP GhcPs]
    declNames (SigD _ d) = sigNameNoLoc d
    declNames _ = []
    -- All documentation of class members is stored in tcdDocs.
    -- To associate the docs with the correct member, we convert all members
    -- and docs to declarations, sort them by their location
    -- and then use collectDocs.
    -- This is the equivalent of Haddock’s Haddock.Interface.Create.classDecls.
    subDecls :: [(HsDecl GhcPs, [T.Text])]
    subDecls = collectDocs . sortOn getLoc $ decls
    decls = docs ++ defs ++ sigs ++ ats
    docs = map (fmap (DocD noExt)) (tcdDocs cls)
    defs = map (fmap (ValD noExt)) (bagToList (tcdMeths cls))
    sigs = map (fmap (SigD noExt)) (tcdSigs cls)
    ats = map (fmap (TyClD noExt . FamDecl noExt)) (tcdATs cls)

-- | This is equivalent to Haddock’s Haddock.Interface.Create.collectDocs
collectDocs :: [LHsDecl a] -> [(HsDecl a, [T.Text])]
collectDocs = go Nothing []
  where
    go :: Maybe (LHsDecl p) -> [T.Text] -> [LHsDecl p] -> [(HsDecl p, [T.Text])]
    go Nothing _ [] = []
    go (Just prev) docs [] = finished prev docs []
    go prev docs (L _ (DocD _ (DocCommentNext str)) : ds)
      | Nothing <- prev = go Nothing (docToText str:docs) ds
      | Just decl <- prev = finished decl docs (go Nothing [docToText str] ds)
    go prev docs (L _ (DocD _ (DocCommentPrev str)) : ds) =
      go prev (docToText str:docs) ds
    go Nothing docs (d:ds) = go (Just d) docs ds
    go (Just prev) docs (d:ds) = finished prev docs (go (Just d) [] ds)

    finished decl docs rest = (unLoc decl, reverse docs) : rest

-- | Parse a module and its dependencies in Haddock mode (retaining Doc
--   declarations), and return the 'ParsedModule's in dependency order (top
--   module last).
--
--   "Internal" modules are filtered out, they don't contribute to the docs.
--
--   Not using the cached file store, as it is expected to run stand-alone
--   invoked by a CLI tool.
haddockParse :: CompileOpts ->
                [FilePath] ->
                Ex.ExceptT DiagnosticStore IO [ParsedModule]
haddockParse opts f = ExceptT $ do
  service <- Service.initialise Service.mainRule Nothing Logger.makeNopHandle opts
  Service.setFilesOfInterest service (Set.fromList f)
  parsed  <- Service.runAction service $
             Ex.runExceptT $
             -- We only _parse_ the modules, the documentation generator is syntax-directed
             do deps <- Service.usesE Service.GetDependencies f
                Service.usesE Service.GetParsedModule $ nubOrd $ f ++ concatMap Service.transitiveModuleDeps deps
                -- The DAML compiler always parses with Opt_Haddock on
  diags <- Service.getDiagnostics service
  pure (mapLeft (const diags) parsed)

-- | Pair up all doc decl.s from a parsed module with their referred-to
--   declaration. See Haddock's equivalent Haddock.Interface.Create.collectDocs
pairDeclDocs :: ParsedModule -> [(HsDecl GhcPs, [T.Text])]
pairDeclDocs ParsedModule{..} = collectDocs (hsmodDecls . unLoc $ pm_parsed_source)

------------------------------------------------------------

-- | Extracts the documentation of a function. Comments are either
--   adjacent to a type signature, or to the actual function definition. If
--   neither a comment nor a function type is in the source, we omit the
--   function.
getFctDocs :: (HsDecl GhcPs, [T.Text]) ->
               Maybe FunctionDoc -- (IdP GhcPs, Maybe (LHsType GhcPs), [T.Text])
getFctDocs (decl, docs) = do
  (name, mbType) <- case decl of
    SigD _ (TypeSig _ (L _ n :_) t) ->
      Just (n, Just . hsib_body . hswc_body $ t)
    SigD _ (ClassOpSig _ _ (L _ n :_) t) ->
      Just (n, Just . hsib_body $ t)
    ValD _ FunBind{..} | not (null docs) ->
      Just (unLoc fun_id, Nothing)
      -- NB assuming we do _not_ have a type signature for the function in the
      -- pairs (otherwise we'll get a duplicate)
    _ ->
      Nothing
  Just $ FunctionDoc
    { fct_name = idpToText name
    , fct_context = hsTypeToContext =<< mbType
    , fct_type = fmap hsTypeToType mbType
    , fct_descr = if null docs then Nothing
                  else Just . T.strip $ T.unlines docs
    }

getClsDocs :: (HsDecl GhcPs, [T.Text]) ->
               Maybe ClassDoc
getClsDocs (TyClD _ c@ClassDecl{..}, docs) = Just ClassDoc
      {cl_name = idpToText $ unLoc tcdLName
      ,cl_descr = if null docs then Nothing else Just $ T.strip $ T.unlines docs
      ,cl_super = case unLoc tcdCtxt of
        [] -> Nothing
        xs -> Just $ TypeTuple $ map hsTypeToType xs
      ,cl_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
      ,cl_functions = concatMap f tcdSigs
      }
  where
    f :: LSig GhcPs -> [FunctionDoc]
    f (L _ (ClassOpSig p b names n)) = catMaybes
      [ getFctDocs (SigD noExt (ClassOpSig p b [L loc name] n), MS.findWithDefault [] name subdocs)
      | L loc name <- names
      ]
    f _ = []
    subdocs = memberDocs c
getClsDocs _ = Nothing

getTypeDocs :: (HsDecl GhcPs, [T.Text]) -> Maybe (IdP GhcPs, ADTDoc)
getTypeDocs (TyClD _ decl, descrs)
  | XTyClDecl{} <- decl =
      Nothing
  | ClassDecl{} <- decl =
      Nothing
  | FamDecl{}   <- decl =
      Nothing

  | SynDecl{..} <- decl =
      let name = idpToText $ unLoc tcdLName
      in Just . (unLoc tcdLName,) $ TypeSynDoc
         { ad_name = name
         , ad_descr = if null descrs then Nothing
                      else Just . T.strip $ T.unlines descrs
         , ad_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
         , ad_rhs  = hsTypeToType tcdRhs
         }

  | DataDecl{..} <- decl =
      let name = idpToText $ unLoc tcdLName
      in Just . (unLoc tcdLName,) $ ADTDoc
         { ad_name = name
         , ad_args   = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
         , ad_descr  = if null descrs then Nothing
                       else Just . T.strip $ T.unlines descrs
         , ad_constrs = map constrDoc . dd_cons $ tcdDataDefn
         }
  where
    constrDoc :: LConDecl GhcPs -> ADTConstr
    constrDoc (L _ con) =
          case con_args con of
            PrefixCon args ->
              PrefixC { ac_name = idpToText . unLoc $ con_name con
                      , ac_descr = fmap (docToText . unLoc) $ con_doc con
                      , ac_args = map hsTypeToType args
                      }
            InfixCon l r ->
              PrefixC { ac_name = idpToText . unLoc $ con_name con
                      , ac_descr = fmap (docToText . unLoc) $ con_doc con
                      , ac_args = map hsTypeToType [l, r]
                      }
            RecCon (L _ fs) ->
              RecordC { ac_name  = idpToText . unLoc $ con_name con
                      , ac_descr = fmap (docToText . unLoc) $ con_doc con
                      , ac_fields = mapMaybe (fieldDoc . unLoc) fs
                      }

    fieldDoc :: ConDeclField GhcPs -> Maybe FieldDoc
    fieldDoc ConDeclField{..} =
      Just $ FieldDoc
      { fd_name = T.concat . map (toText . unLoc) $ cd_fld_names -- FIXME why more than one?
      , fd_type = hsTypeToType cd_fld_type
      , fd_descr = fmap (docToText . unLoc) cd_fld_doc
      }
    fieldDoc XConDeclField{}  = Nothing
getTypeDocs _other = Nothing

getTemplateDocs :: MS.Map RdrName ADTDoc -> Set.Set RdrName -> MS.Map RdrName (Set.Set RdrName) -> [TemplateDoc]
getTemplateDocs typeMap templates choiceMap = map mkTemplateDoc $ Set.toList templates
  where
    -- The following functions use the type map and choice map in scope, so
    -- defined internally, and not expected to fail on consistent arguments.
    mkTemplateDoc :: IdP GhcPs -> TemplateDoc
    mkTemplateDoc name = TemplateDoc
      { td_name  = ad_name tmplADT
      , td_descr = ad_descr tmplADT
      , td_payload = getFields tmplADT
      -- assumes exactly one record constructor (syntactic, template syntax)
      , td_choices = map mkChoiceDoc choices
      }
      where
        tmplADT = asADT name
        choices = Set.toList . fromMaybe Set.empty $ MS.lookup name choiceMap

    mkChoiceDoc :: IdP GhcPs -> ChoiceDoc
    mkChoiceDoc name = ChoiceDoc
      { cd_name = ad_name choiceADT
      , cd_descr = ad_descr choiceADT
      -- assumes exactly one constructor (syntactic in the template syntax), or
      -- uses a dummy value otherwise.
      , cd_fields = getFields choiceADT
      }
          where choiceADT = asADT name

    asADT n = fromMaybe dummyDT $
              MS.lookup n typeMap
    -- returns a dummy ADT if the choice argument is not in the local type map
    -- (possible if choice instances are defined directly outside the template).
    -- This wouldn't be necessary if we used the type-checked AST.
      where dummyDT = ADTDoc { ad_name    = "External:" <> idpToText n
                             , ad_descr   = Nothing
                             , ad_args = []
                             , ad_constrs = []
                             }
    -- Assuming one constructor (record or prefix), extract the fields, if any.
    -- For choices without arguments, GHC returns a prefix constructor, so we
    -- need to cater for this case specially.
    getFields adt = case ad_constrs adt of
                      [PrefixC{}] -> []
                      [RecordC{ ac_fields = fields }] -> fields
                      [] -> [] -- catching the dummy case here, see above
                      _other -> error "getFields: found multiple constructors"



-- recognising Template and Choice instances

-- | Extracts all names of Template instances defined in a module and a map of
-- template to set of its choices (instances of Choice with a particular
-- template).
getTemplateData :: ParsedModule ->
                   (Set.Set (IdP GhcPs), MS.Map (IdP GhcPs) (Set.Set (IdP GhcPs)))
getTemplateData ParsedModule{..} =
  let
    instDs    = mapMaybe (isInstDecl . unLoc) . hsmodDecls . unLoc $ pm_parsed_source
    templates = mapMaybe isTemplate instDs
    choiceMap = MS.fromListWith (<>) $
                map (second Set.singleton) $
                mapMaybe isChoice instDs
  in
    (Set.fromList templates, choiceMap)
    where
      isInstDecl (InstD _ (ClsInstD _ i)) = Just i
      isInstDecl _ = Nothing


-- | If the given instance declaration is declaring a template instance, return
--   its name (IdP). Used to build the set of templates declared in a module.
isTemplate :: ClsInstDecl GhcPs -> Maybe (IdP GhcPs)
isTemplate (XClsInstDecl _) = Nothing
isTemplate ClsInstDecl{..}
  | HsIB _ (L _ ty) <-  cid_poly_ty
  , HsAppTy _ (L _ t1) (L _ t2) <- ty
  , HsTyVar _ _ (L _ tmplClass) <- t1
  , HsTyVar _ _ (L _ tmplName) <- t2
  , toText tmplClass == "DA.Internal.Desugar.Template"
  = Just tmplName

  | otherwise = Nothing

-- | If the given instance declaration is declaring a template choice instance,
--   return template and choice name (IdP). Used to build the set of choices
--   per template declared in a module.
isChoice :: ClsInstDecl GhcPs -> Maybe (IdP GhcPs, IdP GhcPs)
isChoice (XClsInstDecl _) = Nothing
isChoice ClsInstDecl{..}
  | HsIB _ (L _ ty) <-  cid_poly_ty
  , HsAppTy _ (L _ cApp1) (L _ _cArgs) <- ty
  , HsAppTy _ (L _ cApp2) (L _ cName) <- cApp1
  , HsAppTy _ (L _ choice) (L _ cTmpl) <- cApp2
  , HsTyVar _ _ (L _ choiceClass) <- choice
  , HsTyVar _ _ (L _ choiceName) <- cName
  , HsTyVar _ _ (L _ tmplName) <- cTmpl
  , toText choiceClass == "DA.Internal.Desugar.Choice"
  = Just (tmplName, choiceName)

  | otherwise = Nothing


------------------------------------------------------------
-- Generating doc.s from parsed modules

-- render type variables as text (ignore kind information)
tyVarText :: HsTyVarBndr GhcPs -> T.Text
tyVarText arg = case arg of
                  UserTyVar _ (L _ idp)         -> idpToText idp
                  KindedTyVar _ (L _ idp) _kind -> idpToText idp
                  XTyVarBndr _
                    -> error "unexpected X thing"


-- | Converts and trims the bytestring of a doc. decl to Text.
docToText :: HsDocString -> T.Text
docToText = T.strip . T.unlines . go . T.lines . T.pack . unpackHDS
  where
    -- For a haddock comment of the form
    --
    -- -- | First line
    -- --   second line
    -- --   third line
    --
    -- we strip all whitespace from the first line and then on the following
    -- lines we strip at most as much whitespace as we find on the next
    -- non-whitespace line. In the example above, this would result
    -- in the string "First line\nsecond line\n third line".
    -- Trailing whitespace is always stripped.
    go :: [T.Text] -> [T.Text]
    go [] = []
    go (x:xs) = case span isWhitespace xs of
      (_, []) -> [T.strip x]
      (allWhitespace, ls@(firstNonWhitespace : _)) ->
        let limit = T.length (T.takeWhile isSpace firstNonWhitespace)
        in T.strip x : map (const "") allWhitespace ++ map (stripLine limit ) ls
    isWhitespace = T.all isSpace
    stripLine limit = T.stripEnd . stripLeading limit
    stripLeading limit = T.pack . map snd . dropWhile (\(i, c) -> i < limit && isSpace c) . zip [0..] . T.unpack

-- | show a parsed ID (IdP GhcPs == RdrName) as a string
idpToText :: IdP GhcPs -> T.Text
idpToText = T.pack . Out.showSDocUnsafe . Out.ppr


---------------------------------------------------------------------

hsTypeToContext :: LHsType GhcPs -> Maybe DDoc.Type
hsTypeToContext (L _ HsQualTy{..}) = case unLoc hst_ctxt of
    [] -> Nothing
    xs -> Just $ TypeTuple $ map hsTypeToType xs
hsTypeToContext _ = Nothing


hsTypeToType :: LHsType GhcPs -> DDoc.Type
hsTypeToType (L _ t) = hsTypeToType_ t

hsTypeToType_ :: HsType GhcPs -> DDoc.Type
hsTypeToType_ t = case t of

  -- drop context things
  HsForAllTy{..} -> hsTypeToType hst_body
  HsQualTy {..} -> hsTypeToType hst_body
  HsBangTy _ _b ty -> hsTypeToType ty

  -- drop comments (we might want to re-add those at some point)
  HsDocTy _ ty _doc -> hsTypeToType ty

  -- special tuple syntax
  HsTupleTy _ _con tys -> TypeTuple $ map hsTypeToType tys

  -- GHC specials. FIXME deal with them specially
  HsRecTy _ _flds -> TypeApp (toText t) [] -- FIXME pprConDeclFields flds

  HsSumTy _ _tys -> undefined -- FIXME tupleParens UnboxedTuple (pprWithBars ppr tys)


  -- straightforward base case
  HsTyVar _ _ (L _ name) -> TypeApp (idpToText name) []

  HsFunTy _ ty1 ty2 -> case hsTypeToType ty2 of
    TypeFun as -> TypeFun $ hsTypeToType ty1 : as
    ty22 -> TypeFun [hsTypeToType ty1, ty22]

  HsKindSig _ ty _kind -> hsTypeToType ty

  HsListTy _ ty -> TypeList (hsTypeToType ty)

  HsIParamTy _ _n ty -> hsTypeToType ty
  -- currently bailing out when we meet promoted structures
  HsSpliceTy _ _s     -> unexpected "splice"
  HsExplicitListTy _ _ _tys ->  unexpected "explicit list"
  HsExplicitTupleTy _ _tys  -> unexpected "explicit tuple"

  HsTyLit _ ty      -> TypeApp (toText ty) []
  -- kind things. Can be printed, not sure why we would
  HsWildCardTy {}   -> TypeApp "_" []
  HsStarTy _ _      -> TypeApp "*" []

  HsAppTy _ fun_ty arg_ty ->
    case hsTypeToType fun_ty of
      TypeApp f as -> TypeApp f $ as <> [hsTypeToType arg_ty]  -- flattens app chains
      TypeFun _ -> unexpected "function type in a type app"
      TypeList _   -> unexpected "list type in a type app"
      TypeTuple _   -> unexpected "tuple type in a type app"

  HsAppKindTy _ _ _ -> unexpected "kind application"

  HsOpTy _ ty1 (L _ op) ty2 ->
    TypeApp (toText op) [ hsTypeToType ty1, hsTypeToType ty2 ]

  HsParTy _ ty  -> hsTypeToType ty

  XHsType _t -> unexpected "XHsType"

  where unexpected x = error $ "hsTypeToType: found an unexpected " <> x

---- HACK ZONE --------------------------------------------------------

-- Generic ppr for various things we need as text.
-- FIXME Replace by specialised functions for the particular things.
toText :: Out.Outputable a => a -> T.Text
toText = T.pack . Out.showSDocOneLine DF.unsafeGlobalDynFlags . Out.ppr
