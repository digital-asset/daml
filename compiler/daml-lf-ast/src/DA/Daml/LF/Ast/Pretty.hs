-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE PatternSynonyms #-}
module DA.Daml.LF.Ast.Pretty(
    prettyName,
    prettyDottedName,
    prettyQualified,
    (<:>)
    ) where

import DA.Prelude

import qualified Data.Ratio                 as Ratio
import           Control.Lens
import           Control.Lens.Ast   (rightSpine)
import qualified Data.NameMap as NM
import qualified Data.Text          as T
import qualified Data.Time.Clock.POSIX      as Clock.Posix
import qualified Data.Time.Format           as Time.Format
import           Data.Foldable (toList)

import           DA.Daml.LF.Ast.Base hiding (dataCons)
import           DA.Daml.LF.Ast.Util
import           DA.Daml.LF.Ast.Optics
import           DA.Pretty hiding (keyword_, type_)

infixr 6 <:>
(<:>) :: Doc ann -> Doc ann -> Doc ann
x <:> y = x <-> ":" <-> y

keyword_ :: String -> Doc ann
keyword_ = string

kind_ :: Doc ann -> Doc ann
kind_ = id

type_ :: Doc ann -> Doc ann
type_ = id

prettyName :: Tagged tag T.Text -> Doc ann
prettyName = pretty . unTagged

prettyDottedName :: Tagged tag [T.Text] -> Doc ann
prettyDottedName = hcat . punctuate "." . map pretty . unTagged

instance Pretty TypeConName where
  pPrint = prettyDottedName

prettyModuleRef :: (PackageRef, ModuleName) -> Doc ann
prettyModuleRef (pkgRef, modName) = docPkgRef <> prettyDottedName modName
  where
    docPkgRef = case pkgRef of
      PRSelf         -> mempty
      PRImport pkgId -> prettyName pkgId <> ":"

prettyQualified :: (a -> Doc ann) -> Qualified a -> Doc ann
prettyQualified prettyA (Qualified pkgRef modName x) =
  prettyModuleRef (pkgRef, modName) <> ":" <> prettyA x

instance Pretty a => Pretty (Qualified a) where
  pPrint = prettyQualified pretty

instance Pretty SourceLoc where
  pPrint (SourceLoc mbModRef slin scol elin ecol) =
    hcat
    [ maybe mempty (\modRef -> prettyModuleRef modRef <> ":") mbModRef
    , int slin, ":", int scol, "-", int elin, ":", int ecol
    ]

withSourceLoc :: Maybe SourceLoc -> Doc ann -> Doc ann
withSourceLoc mbLoc doc =
  maybe doc (\loc -> "@location" <> parens (pretty loc) $$ doc) mbLoc

precHighest, precKArrow, precTApp, precTFun, precTForall :: Rational
precHighest = 1000  -- NOTE(MH): Used for type applications in 'Expr'.
precKArrow  = 0
precTApp    = 2
precTFun    = 1
precTForall = 0

prettyFunArrow, prettyForall, prettyHasType :: Doc ann
prettyFunArrow = "->"
prettyForall   = "forall"
prettyHasType  = ":"

instance Pretty Kind where
  pPrintPrec lvl prec = \case
    KStar -> "*"
    KArrow k1 k2 ->
      maybeParens (prec > precKArrow) $
        pPrintPrec lvl (succ precKArrow) k1 <-> prettyFunArrow <-> pPrintPrec lvl precKArrow k2

-- FIXME(MH): Use typeConAppToType.
instance Pretty TypeConApp where
  pPrintPrec lvl prec (TypeConApp con args) =
    maybeParens (prec > precTApp && not (null args)) $
      prettyQualified prettyDottedName con
      <-> hsep (map (pPrintPrec lvl (succ precTApp)) args)

instance Pretty EnumType where
  pPrint = \case
    ETUnit -> "Unit"
    ETBool -> "Bool"

instance Pretty BuiltinType where
  pPrint = \case
    BTInt64          -> "Int64"
    BTDecimal        -> "Decimal"
    BTText           -> "Text"
    BTTimestamp      -> "Timestamp"
    BTParty          -> "Party"
    BTEnum etype -> pretty etype
    BTList -> "List"
    BTUpdate -> "Update"
    BTScenario -> "Scenario"
    BTDate           -> "Date"
    BTContractId -> "ContractId"
    BTOptional -> "Optional"
    BTMap -> "Map"
    BTArrow -> "(->)"

prettyRecord :: (Pretty a) =>
  Doc ann -> [(FieldName, a)] -> Doc ann
prettyRecord sept fields =
  braces (sep (punctuate ";" (map prettyField fields)))
  where
    prettyField (name, thing) = hang (prettyName name <-> sept) 2 (pretty thing)

prettyTuple :: (Pretty a) =>
  Doc ann -> [(FieldName, a)] -> Doc ann
prettyTuple sept fields =
  "<" <> sep (punctuate ";" (map prettyField fields)) <> ">"
  where
    prettyField (name, thing) = hang (prettyName name <-> sept) 2 (pretty thing)

instance Pretty Type where
  pPrintPrec lvl prec = \case
    TVar v -> prettyName v
    TCon c -> prettyQualified prettyDottedName c
    TApp (TApp (TBuiltin BTArrow) tx) ty ->
      maybeParens (prec > precTFun)
        (pPrintPrec lvl (succ precTFun) tx <-> prettyFunArrow <-> pPrintPrec lvl precTFun ty)
    TApp tf ta ->
      maybeParens (prec > precTApp) $
        pPrintPrec lvl precTApp tf <-> pPrintPrec lvl (succ precTApp) ta
    TBuiltin b -> pretty b
    t0@TForall{} ->
      let (vs, t1) = view _TForalls t0
      in  maybeParens (prec > precTForall)
            (prettyForall <-> hsep (map prettyNameAndKind vs) <> "."
             <-> pPrintPrec lvl precTForall t1)
    TTuple fields -> prettyTuple prettyHasType fields

precEApp, precEAbs :: Rational
precEApp = 2
precEAbs = 0

prettyLambda, prettyTyLambda, prettyLambdaDot, prettyTyLambdaDot, prettyAltArrow :: Doc ann
prettyLambda      = "\\"
prettyTyLambda    = "/\\"
prettyLambdaDot   = "."
prettyTyLambdaDot = "."
prettyAltArrow    = "->"

instance Pretty PartyLiteral where
  pPrint = quotes . pretty . unTagged

instance Pretty EnumCon where
  pPrint = \case
    ECUnit  -> "Unit"
    ECFalse -> "False"
    ECTrue  -> "True"

instance Pretty BuiltinExpr where
  pPrintPrec _lvl prec = \case
    BEInt64 n -> pretty (toInteger n)
    BEDecimal dec -> string (show dec)
    BEText t -> string (show t) -- includes the double quotes, and escapes characters
    BEParty p -> pretty p
    BEEnumCon c -> pretty c
    BEError -> "ERROR"
    BEEqual t     -> maybeParens (prec > precEApp) ("EQUAL"      <-> prettyBTyArg t)
    BELess t      -> maybeParens (prec > precEApp) ("LESS"       <-> prettyBTyArg t)
    BELessEq t    -> maybeParens (prec > precEApp) ("LESS_EQ"    <-> prettyBTyArg t)
    BEGreater t   -> maybeParens (prec > precEApp) ("GREATER"    <-> prettyBTyArg t)
    BEGreaterEq t -> maybeParens (prec > precEApp) ("GREATER_EQ" <-> prettyBTyArg t)
    BEToText t    -> maybeParens (prec > precEApp) ("TO_TEXT"    <-> prettyBTyArg t)
    BEAddDecimal -> "ADD_DECIMAL"
    BESubDecimal -> "SUB_DECIMAL"
    BEMulDecimal -> "MUL_DECIMAL"
    BEDivDecimal -> "DIV_DECIMAL"
    BERoundDecimal -> "ROUND_DECIMAL"
    BEAddInt64 -> "ADD_INT64"
    BESubInt64 -> "SUB_INT64"
    BEMulInt64 -> "MUL_INT64"
    BEDivInt64 -> "DIV_INT64"
    BEModInt64 -> "MOD_INT64"
    BEExpInt64 -> "EXP_INT64"
    BEFoldl -> "FOLDL"
    BEFoldr -> "FOLDR"
    BEMapEmpty -> "MAP_EMPTY"
    BEMapInsert -> "MAP_INSERT"
    BEMapLookup -> "MAP_LOOKUP"
    BEMapDelete -> "MAP_DELETE"
    BEMapSize -> "MAP_SIZE"
    BEMapToList -> "MAP_TO_LIST"
    BEEqualList -> "EQUAL_LIST"
    BEAppendText -> "APPEND_TEXT"
    BETimestamp ts -> pretty (timestampToText ts)
    BEDate date -> pretty (dateToText date)
    BEInt64ToDecimal -> "INT64_TO_DECIMAL"
    BEDecimalToInt64 -> "DECIMAL_TO_INT64"
    BETimestampToUnixMicroseconds -> "TIMESTAMP_TO_UNIX_MICROSECONDS"
    BEUnixMicrosecondsToTimestamp -> "UNIX_MICROSECONDS_TO_TIMESTAMP"
    BEDateToUnixDays -> "DATE_TO_UNIX_DAYS"
    BEUnixDaysToDate -> "UNIX_DAYS_TO_DATE"
    BEExplodeText -> "EXPLODE_TEXT"
    BEImplodeText -> "IMPLODE_TEXT"
    BESha256Text -> "SHA256_TEXT"
    BETrace -> "TRACE"
    BEEqualContractId -> "EQUAL_CONTRACT_ID"
    BEPartyFromText -> "PARTY_FROM_TEXT"
    BEPartyToQuotedText -> "PARTY_TO_QUOTED_TEXT"
    where
      epochToText fmt secs =
        T.pack $
        Time.Format.formatTime Time.Format.defaultTimeLocale fmt $
        Clock.Posix.posixSecondsToUTCTime (fromRational secs)

      timestampToText micros =
        epochToText "%0Y-%m-%dT%T%QZ" (toInteger micros Ratio.% (10 ^ (6 :: Integer)))

      dateToText days = epochToText "%0Y-%m-%d" ((toInteger days * 24 * 60 * 60) Ratio.% 1)

prettyNameAndKind :: (Tagged tag T.Text, Kind) -> Doc ann
prettyNameAndKind (v, k) = case k of
    KStar -> prettyName v
    _ -> parens (prettyName v <-> prettyHasType <-> kind_ (pretty k))

prettyNameAndType :: (Tagged tag T.Text, Type) -> Doc ann
prettyNameAndType (x, t) = prettyName x <-> prettyHasType <-> type_ (pretty t)

instance Pretty CasePattern where
  pPrint = \case
    CPVariant tcon con var ->
      prettyQualified prettyDottedName tcon <> "." <> prettyName con
      <-> prettyName var
    CPEnumCon con -> pretty con
    CPNil -> keyword_ "nil"
    CPCons hdVar tlVar -> keyword_ "cons" <-> prettyName hdVar <-> prettyName tlVar
    CPDefault -> keyword_ "default"
    CPNone -> keyword_ "none"
    CPSome bodyVar -> keyword_ "some" <-> prettyName bodyVar

instance Pretty CaseAlternative where
  pPrint (CaseAlternative pat expr) =
    hang (pretty pat <-> prettyAltArrow) 2 (pretty expr)

instance Pretty Binding where
  pPrint (Binding binder expr) =
    hang (prettyNameAndType binder <-> "=") 2 (pretty expr)

prettyTyArg :: Type -> Doc ann
prettyTyArg t = type_ ("@" <> pPrintPrec prettyNormal precHighest t)

prettyBTyArg :: BuiltinType -> Doc ann
prettyBTyArg = prettyTyArg . TBuiltin

prettyTmArg :: Expr -> Doc ann
prettyTmArg = pPrintPrec prettyNormal (succ precEApp)

tplArg :: Qualified TypeConName -> Arg
tplArg tpl = TyArg (TCon tpl)

instance Pretty Arg where
  pPrint = \case
    TmArg e -> prettyTmArg e
    TyArg t -> prettyTyArg t

prettyAppDoc :: Rational -> Doc ann -> [Arg] -> Doc ann
prettyAppDoc prec d as = maybeParens (prec > precEApp) $
  sep (d : map (nest 2 . pretty) as)

prettyAppKeyword :: Rational -> String -> [Arg] -> Doc ann
prettyAppKeyword prec kw = prettyAppDoc prec (keyword_ kw)

prettyApp :: Rational -> Expr -> [Arg] -> Doc ann
prettyApp prec f = prettyAppDoc prec (pPrintPrec prettyNormal precEApp f)

instance Pretty Update where
  pPrintPrec _lvl prec = \case
    UPure typ arg ->
      prettyAppKeyword prec "upure" [TyArg typ, TmArg arg]
    upd@UBind{} -> maybeParens (prec > precEAbs) $
      let (binds, body) = view (rightSpine (unlocate $ _EUpdate . _UBind)) (EUpdate upd)
      in  keyword_ "ubind" <-> vcat (map pretty binds)
          $$ keyword_ "in" <-> pretty body
    UCreate tpl arg ->
      prettyAppKeyword prec "create" [tplArg tpl, TmArg arg]
    UExercise tpl choice cid actor arg ->
      -- NOTE(MH): Converting the choice name into a variable is a bit of a hack.
      prettyAppKeyword prec "exercise"
      [tplArg tpl, TmArg (EVar (retag choice)), TmArg cid, TmArg actor, TmArg arg]
    UFetch tpl cid ->
      prettyAppKeyword prec "fetch" [tplArg tpl, TmArg cid]
    UGetTime ->
      keyword_ "get_time"
    UEmbedExpr typ e ->
      prettyAppKeyword prec "uembed_expr" [TyArg typ, TmArg e]
    UFetchByKey RetrieveByKey{..} ->
      prettyAppKeyword prec "ufetch_by_key" [tplArg retrieveByKeyTemplate, TmArg retrieveByKeyKey]
    ULookupByKey RetrieveByKey{..} ->
      prettyAppKeyword prec "ulookup_by_key" [tplArg retrieveByKeyTemplate, TmArg retrieveByKeyKey]

instance Pretty Scenario where
  pPrintPrec _lvl prec = \case
    SPure typ arg ->
      prettyAppKeyword prec "spure" [TyArg typ, TmArg arg]
    scen@SBind{} -> maybeParens (prec > precEAbs) $
      let (binds, body) = view (rightSpine (_EScenario . _SBind)) (EScenario scen)
      in  keyword_ "sbind" <-> vcat (map pretty binds)
          $$ keyword_ "in" <-> pretty body
    SCommit typ actor upd ->
      prettyAppKeyword prec "commit" [TyArg typ, TmArg actor, TmArg upd]
    SMustFailAt typ actor upd ->
      prettyAppKeyword prec "must_fail_at" [TyArg typ, TmArg actor, TmArg upd]
    SPass delta ->
      prettyAppKeyword prec "pass" [TmArg delta]
    SGetTime ->
      keyword_ "get_time"
    SGetParty name ->
      prettyAppKeyword prec "get_party" [TmArg name]
    SEmbedExpr typ e ->
      prettyAppKeyword prec "sembed_expr" [TyArg typ, TmArg e]

instance Pretty Expr where
  pPrintPrec lvl prec = \case
    EVar x -> prettyName x
    EVal z -> prettyQualified prettyName z
    EBuiltin b -> pPrintPrec lvl prec b
    ERecCon (TypeConApp tcon targs) fields ->
      maybeParens (prec > precEApp) $
        sep $
          prettyQualified prettyDottedName tcon
          : map (nest 2 . prettyTyArg) targs
          ++ [nest 2 (prettyRecord "=" fields)]
    ERecProj (TypeConApp tcon targs) field rec ->
      prettyAppDoc prec
        (prettyQualified prettyDottedName tcon <> "." <> prettyName field)
        (map TyArg targs ++ [TmArg rec])
    ERecUpd (TypeConApp tcon targs) field record update ->
      maybeParens (prec > precEApp) $
        sep $
          prettyQualified prettyDottedName tcon
          : map (nest 2 . prettyTyArg) targs
          ++ [nest 2 (braces updDoc)]
      where
        updDoc = sep
          [ pretty record
          , keyword_ "with"
          , hang (prettyName field <-> "=") 2 (pretty update)
          ]
    EVariantCon (TypeConApp tcon targs) con arg ->
      prettyAppDoc prec
        (prettyQualified prettyDottedName tcon <> ":" <> prettyName con)
        (map TyArg targs ++ [TmArg arg])
    ETupleCon fields ->
      prettyTuple "=" fields
    ETupleProj field expr -> pPrintPrec lvl precHighest expr <> "." <> prettyName field
    ETupleUpd field tuple update ->
          "<" <> updDoc <> ">"
      where
        updDoc = sep
          [ pretty tuple
          , keyword_ "with"
          , hang (prettyName field <-> "=") 2 (pretty update)
          ]
    e@ETmApp{} -> uncurry (prettyApp prec) (e ^. _EApps)
    e@ETyApp{} -> uncurry (prettyApp prec) (e ^. _EApps)
    e0@ETmLam{} -> maybeParens (prec > precEAbs) $
      let (bs, e1) = view (rightSpine (unlocate _ETmLam)) e0
      in  hang (prettyLambda <> hsep (map (parens . prettyNameAndType) bs) <> prettyLambdaDot)
            2 (pretty e1)
    e0@ETyLam{} -> maybeParens (prec > precEAbs) $
      let (ts, e1) = view (rightSpine (unlocate _ETyLam)) e0
      in  hang (prettyTyLambda <> hsep (map prettyNameAndKind ts) <> prettyTyLambdaDot)
            2 (pretty e1)
    ECase scrut alts -> maybeParens (prec > precEApp) $
      keyword_ "case" <-> pretty scrut <-> keyword_ "of"
      $$ nest 2 (vcat (map pretty alts))
    e0@ELet{} -> maybeParens (prec > precEAbs) $
      let (binds, e1) = view (rightSpine (unlocate _ELet)) e0
      in  keyword_ "let" <-> vcat (map pretty binds)
          $$ keyword_ "in" <-> pretty e1
    ENil elemType ->
      prettyAppKeyword prec "nil" [TyArg elemType]
    ECons elemType headExpr tailExpr ->
      prettyAppKeyword prec "cons" [TyArg elemType, TmArg headExpr, TmArg tailExpr]
    EUpdate upd -> pPrintPrec lvl prec upd
    EScenario scen -> pPrintPrec lvl prec scen
    ELocation _ x -> pPrintPrec lvl prec x
    ESome typ body -> prettyAppKeyword prec "some" [TyArg typ, TmArg body]
    ENone typ -> prettyAppKeyword prec "none" [TyArg typ]

instance Pretty DefDataType where
  pPrint (DefDataType mbLoc tcon (IsSerializable serializable) params dataCons) =
    withSourceLoc mbLoc $ case dataCons of
    DataRecord fields ->
      hang (keyword_ "record" <-> lhsDoc) 2 (prettyRecord prettyHasType fields)
    DataVariant variants ->
      (keyword_ "variant" <-> lhsDoc) $$ nest 2 (vcat (map prettyCon variants))
    where
      lhsDoc =
        serializableDoc <-> prettyDottedName tcon <-> hsep (map prettyNameAndKind params) <-> "="
      serializableDoc = if serializable then "@serializable" else mempty
      prettyCon (name, typ) =
        "|" <-> prettyName name <-> pPrintPrec prettyNormal precHighest typ

instance Pretty DefValue where
  pPrint (DefValue mbLoc binder (HasNoPartyLiterals noParties) (IsTest isTest) body) =
    withSourceLoc mbLoc $
    vcat
      [ hang (keyword_ kind <-> annot <-> prettyNameAndType binder <-> "=") 2 (pretty body) ]
    where
      kind = if isTest then "test" else "def"
      annot = if noParties then mempty else "@partyliterals"

prettyTemplateChoice ::
  ModuleName -> TypeConName -> TemplateChoice -> Doc ann
prettyTemplateChoice modName tpl (TemplateChoice mbLoc name isConsuming actor selfBinder argBinder retType update) =
  withSourceLoc mbLoc $
    vcat
    [ hsep
      [ keyword_ "choice"
      , keyword_ (if isConsuming then "consuming" else "non-consuming")
      , prettyName name
      , parens (prettyNameAndType (selfBinder, TContractId (TCon (Qualified PRSelf modName tpl))))
      , parens (prettyNameAndType argBinder), prettyHasType, pretty retType
      ]
    , nest 2 (keyword_ "by" <-> pretty actor)
    , nest 2 (keyword_ "to" <-> pretty update)
    ]

prettyTemplate ::
  ModuleName -> Template -> Doc ann
prettyTemplate modName (Template mbLoc tpl param precond signatories observers agreement choices mbKey) =
  withSourceLoc mbLoc $
    keyword_ "template" <-> prettyDottedName tpl <-> prettyName param
    <-> keyword_ "where"
    $$ nest 2 (vcat ([signatoriesDoc, observersDoc, precondDoc, agreementDoc, choicesDoc] ++ mbKeyDoc))
    where
      signatoriesDoc = keyword_ "signatory" <-> pretty signatories
      observersDoc = keyword_ "observer" <-> pretty observers
      precondDoc = keyword_ "ensure" <-> pretty precond
      agreementDoc = hang (keyword_ "agreement") 2 (pretty agreement)
      choicesDoc = vcat (map (prettyTemplateChoice modName tpl) (NM.toList choices))
      mbKeyDoc = toList $ do
        key <- mbKey
        return $ vcat
          [ keyword_ "key" <-> pretty (tplKeyType key)
          , nest 2 (keyword_ "body" <-> pretty (tplKeyBody key))
          , nest 2 (keyword_ "maintainers" <-> pretty (tplKeyMaintainers key))
          ]

prettyFeatureFlags :: FeatureFlags -> Doc ann
prettyFeatureFlags
  FeatureFlags
  { forbidPartyLiterals } =
  fcommasep $ catMaybes
    [ optionalFlag forbidPartyLiterals "+ForbidPartyLiterals"
    ]
  where
    optionalFlag flag name
      | flag = Just name
      | otherwise = Nothing

instance Pretty Module where
  pPrint (Module modName _path flags dataTypes values templates) =
    vsep $ moduleHeader ++  map (nest 2) defns
    where
      defns = concat
        [ map pretty (NM.toList dataTypes)
        , map pretty (NM.toList values)
        , map (prettyTemplate modName) (NM.toList templates)
        ]
      prettyFlags = prettyFeatureFlags flags
      moduleHeader
        | isEmpty prettyFlags = [keyword_ "module" <-> prettyDottedName modName <-> keyword_ "where"]
        | otherwise = [prettyFlags, keyword_ "module" <-> prettyDottedName modName <-> keyword_ "where"]

instance Pretty Package where
  pPrint (Package version modules) =
    vcat
      [ "daml-lf" <-> pretty version
      , vsep $ map pretty (NM.toList modules)
      ]
