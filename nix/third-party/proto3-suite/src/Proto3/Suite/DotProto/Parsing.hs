-- | This module contains a near-direct translation of the proto3 grammar
--   It uses String for easier compatibility with DotProto.Generator, which needs it for not very good reasons

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# OPTIONS_GHC -fno-warn-unused-do-bind #-}

module Proto3.Suite.DotProto.Parsing
  ( parseProto
  , parseProtoFile
  ) where

import Control.Applicative hiding (empty)
import Data.Functor
import qualified Data.Text as T
import qualified Filesystem.Path.CurrentOS as FP
import Proto3.Suite.DotProto.AST
import Proto3.Wire.Types (FieldNumber(..))
import Text.Parsec (parse, ParseError)
import Text.Parsec.String (Parser)
import Text.Parser.Char
import Text.Parser.Combinators
import Text.Parser.LookAhead
import Text.Parser.Token
import qualified Turtle

----------------------------------------
-- interfaces

-- | @parseProto mp inp@ attempts to parse @inp@ as a 'DotProto'. @mp@ is the
-- module path to be injected into the AST as part of 'DotProtoMeta' metadata on
-- a successful parse.
parseProto :: Path -> String -> Either ParseError DotProto
parseProto modulePath = parse (topLevel modulePath) "" . stripComments

-- | @parseProtoFile mp fp@ reads and parses the .proto file found at @fp@. @mp@
-- is used downstream during code generation when we need to generate names
-- which are a function of the source .proto file's filename and its path
-- relative to some @--includeDir@.
parseProtoFile :: Turtle.MonadIO m
               => Path -> Turtle.FilePath -> m (Either ParseError DotProto)
parseProtoFile modulePath =
  fmap (parseProto modulePath) . Turtle.liftIO . readFile . FP.encodeString

----------------------------------------
-- convenience

listSep :: Parser ()
listSep = whiteSpace >> text "," >> whiteSpace

empty :: Parser ()
empty = whiteSpace >> text ";" >> return ()

fieldNumber :: Parser FieldNumber
fieldNumber = FieldNumber . fromInteger <$> integer

-- [issue] this is a terrible, naive way to strip comments
--         any string that contains "//" breaks
--         a regex would be better, but the best thing to do is to just replace all the string logic with a lexer
_stripComments :: String -> String
_stripComments ('/':'/':rest) = _stripComments (dropWhile (/= '\n') rest)
_stripComments (x:rest)       = x:_stripComments rest
_stripComments []             = []

-- [issue] This is still a terrible and naive way to strip comments, was written
-- hastily, and has been only lightly tested. However, it improves upon
-- `_stripComments` above: it handles /* block comments /* with nesting */ */,
-- "// string lits with comments in them", etc., and thus is closer to the
-- protobuf3 grammar. The right solution is still to replace this with a proper
-- lexer, but since we might switch to using the `protoc`-based `FileDescriptor`
-- parsing instead, we should hold off. If we do decide to stick with this
-- parser, we should also inject comments into the AST so that they can be
-- marshaled into the generated code, and ensure that line numbers reported in
-- errors are still correct. Although this implementation is handy to toss out
-- in the interests of getting some more .protos parsable, it's probably not
-- worth maintaining, and has an ad-hoc smell. If we find ourselves mucking with
-- this much at all in the very near short term, let's just pay the freight and
-- use `Text.Parsec.Token` or somesuch.
data StripCommentState
       --               | starts  | ends | error on?       |
  = BC -- block comment | "/*"    | "*/" | mismatch        |
  | DQ -- double quote  | '"'     | '"'  | mismatch        |
  | LC -- line comment  | "//"    | '\n' | mismatch ('\n') |
  deriving Show

stripComments :: String -> String
stripComments = go [] where
  go []        ('/':'*':cs) = go [BC] cs
  go st@(BC:_) ('/':'*':cs) = go (BC:st) cs
  go []        ('*':'/':_)  = error "*/ without preceding /*"
  go (BC:st)   ('*':'/':cs) = go st cs
  go st@(BC:_) (_:cs)       = go st cs
  go []        ('/':'/':cs) = go [LC] cs
  go []        ('"':cs)     = '"' : go [DQ] cs
  go (LC:st)   cs@('\n':_)  = go st cs
  go st@(LC:_) (_:cs)       = go st cs
  go (DQ:st)   ('"':cs)     = '"' : go st cs
  go st        (c:cs)       = c : go st cs
  go []        []           = []
  go (BC:_)    []           = error "unterminated block comment"
  go (DQ:_)    []           = error "unterminated double-quote"
  go (LC:_)    []           = error "unterminated line comment (missing newline)"

----------------------------------------
-- identifiers

identifierName :: Parser String
identifierName = do h <- letter
                    t <- many (alphaNum <|> char '_')
                    return $ h:t

identifier :: Parser DotProtoIdentifier
identifier = do is <- identifierName `sepBy1` string "."
                return $ case is of
                  [i] -> Single i
                  _   -> Dots (Path is)

-- [note] message and enum types are defined by the proto3 spec to have an optional leading period (messageType and enumType in the spec)
--        what this indicates is, as far as i can tell, not documented, and i haven't found this syntax used in practice
--        it's ommitted but can be fairly easily added if there is in fact a use for it

-- [update] the leading dot denotes that the identifier path starts in global scope
--          i still haven't seen a use case for this but i can add it upon request

nestedIdentifier :: Parser DotProtoIdentifier
nestedIdentifier = do h <- parens identifier
                      string "."
                      t <- identifier
                      return $ Qualified h t

----------------------------------------
-- values

-- [issue] these string parsers are weak to \" and \000 octal codes
stringLit :: Parser String
stringLit = stringLiteral <|> stringLiteral'

bool :: Parser Bool
bool = (string "true"  >> (notFollowedBy $ alphaNum <|> char '_') $> True) -- used to distinguish "true_" (Identifier) from "true" (BoolLit)
   <|> (string "false" >> (notFollowedBy $ alphaNum <|> char '_') $> False)

-- the `parsers` package actually does not expose a parser for signed fractional values
floatLit :: Parser Double
floatLit = do sign <- char '-' $> negate <|> char '+' $> id <|> pure id
              sign <$> double

value :: Parser DotProtoValue
value = try (BoolLit              <$> bool)
    <|> try (StringLit            <$> stringLit)
    <|> try (FloatLit             <$> floatLit)
    <|> try (IntLit . fromInteger <$> integer)
    <|> try (Identifier           <$> identifier)

----------------------------------------
-- types

primType :: Parser DotProtoPrimType
primType = try (string "double"   $> Double)
       <|> try (string "float"    $> Float)
       <|> try (string "int32"    $> Int32)
       <|> try (string "int64"    $> Int64)
       <|> try (string "sint32"   $> SInt32)
       <|> try (string "sint64"   $> SInt64)
       <|> try (string "uint32"   $> UInt32)
       <|> try (string "uint64"   $> UInt64)
       <|> try (string "fixed32"  $> Fixed32)
       <|> try (string "fixed64"  $> Fixed64)
       <|> try (string "sfixed32" $> SFixed32)
       <|> try (string "sfixed64" $> SFixed64)
       <|> try (string "string"   $> String)
       <|> try (string "bytes"    $> Bytes)
       <|> try (string "bool"     $> Bool)
       <|> Named <$> identifier

--------------------------------------------------------------------------------
-- top-level parser and version annotation

syntaxSpec :: Parser ()
syntaxSpec = do string "syntax"
                whiteSpace
                string "="
                whiteSpace
                string "'proto3'" <|> string "\"proto3\""
                whiteSpace
                string ";"
                whiteSpace

data DotProtoStatement
  = DPSOption     DotProtoOption
  | DPSPackage    DotProtoPackageSpec
  | DPSImport     DotProtoImport
  | DPSDefinition DotProtoDefinition
  | DPSEmpty
  deriving Show

sortStatements :: Path -> [DotProtoStatement] -> DotProto
sortStatements modulePath statements
  = DotProto { protoOptions     =       [ x | DPSOption     x <- statements]
             , protoImports     =       [ x | DPSImport     x <- statements]
             , protoPackage     = adapt [ x | DPSPackage    x <- statements]
             , protoDefinitions =       [ x | DPSDefinition x <- statements]
             , protoMeta        = DotProtoMeta modulePath
             }
  where
    adapt (x:_) = x
    adapt _     = DotProtoNoPackage

topLevel :: Path -> Parser DotProto
topLevel modulePath = do whiteSpace
                         syntaxSpec
                         whiteSpace
                         sortStatements modulePath <$> topStatement `sepBy` whiteSpace

--------------------------------------------------------------------------------
-- top-level statements

topStatement :: Parser DotProtoStatement
topStatement = (DPSImport     <$> import_)
           <|> (DPSPackage    <$> package)
           <|> (DPSOption     <$> topOption)
           <|> (DPSDefinition <$> definition)
           <|> empty $> DPSEmpty

import_ :: Parser DotProtoImport
import_ = do string "import"
             whiteSpace
             qualifier <- option DotProtoImportDefault ((string "weak" $> DotProtoImportWeak) <|> (string "public" $> DotProtoImportPublic))
             whiteSpace
             target <- FP.fromText . T.pack <$> stringLit
             string ";"
             return $ DotProtoImport qualifier target

package :: Parser DotProtoPackageSpec
package = do string "package"
             whiteSpace
             p <- identifier
             whiteSpace
             string ";"
             return $ DotProtoPackageSpec p

definition :: Parser DotProtoDefinition
definition = message
         <|> enum
         <|> service

--------------------------------------------------------------------------------
-- options

optionName :: Parser DotProtoIdentifier
optionName = do ohead <- nestedIdentifier <|> identifier -- this permits the (p.p2).p3 option identifier form
                                                         -- i'm not actually sure if this form is used in non-option statements
                whiteSpace
                string "="
                return ohead

optionValue :: Parser DotProtoValue
optionValue = do whiteSpace
                 v <- value
                 return v

inlineOption :: Parser DotProtoOption
inlineOption = DotProtoOption <$> optionName <*> optionValue

optionAnnotation :: Parser [DotProtoOption]
optionAnnotation = (brackets $ inlineOption `sepBy1` listSep) <|> pure []

topOption :: Parser DotProtoOption
topOption = do string "option"
               whiteSpace
               v <- DotProtoOption <$> optionName <*> optionValue
               whiteSpace
               string ";"
               whiteSpace
               return v

--------------------------------------------------------------------------------
-- service statements

servicePart :: Parser DotProtoServicePart
servicePart = rpc
          <|> (DotProtoServiceOption <$> topOption)
          <|> empty $> DotProtoServiceEmpty

rpcOptions :: Parser [DotProtoOption]
rpcOptions = braces $ many topOption

rpcClause :: Parser (DotProtoIdentifier, Streaming)
rpcClause = do
  let sid ctx = (,ctx) <$> identifier
  -- NB: Distinguish "stream stream.foo" from "stream.foo"
  try (string "stream" *> whiteSpace *> sid Streaming) <|> sid NonStreaming

rpc :: Parser DotProtoServicePart
rpc = do string "rpc"
         whiteSpace
         name <- Single <$> identifierName
         whiteSpace
         subjecttype <- parens rpcClause
         whiteSpace
         string "returns"
         whiteSpace
         returntype <- parens rpcClause
         whiteSpace
         options <- rpcOptions <|> (string ";" $> [])
         return $ DotProtoServiceRPC name subjecttype returntype options

service :: Parser DotProtoDefinition
service = do string "service"
             whiteSpace
             name <- Single <$> identifierName
             whiteSpace
             statements <- braces (servicePart `sepEndBy` whiteSpace)
             return $ DotProtoService name statements

--------------------------------------------------------------------------------
-- message definitions

message :: Parser DotProtoDefinition
message = do string "message"
             whiteSpace
             name <- Single <$> identifierName
             whiteSpace
             body <- braces (messagePart `sepEndBy` whiteSpace)
             return $ DotProtoMessage name body

messagePart :: Parser DotProtoMessagePart
messagePart = try (DotProtoMessageDefinition <$> enum)
          <|> try (DotProtoMessageReserved   <$> reservedField)
          <|> try (DotProtoMessageDefinition <$> message)
          <|> try messageOneOf
          <|> try (DotProtoMessageField      <$> messageMapField)
          <|>     (DotProtoMessageField      <$> messageField)

messageField :: Parser DotProtoField
messageField = do ctor <- (try $ string "repeated" $> Repeated) <|> pure Prim
                  whiteSpace
                  mtype <- primType
                  whiteSpace
                  mname <- identifier
                  whiteSpace
                  string "="
                  whiteSpace
                  mnumber <- fieldNumber
                  whiteSpace
                  moptions <- optionAnnotation
                  whiteSpace
                  string ";"
                  -- TODO: parse comments
                  return $ DotProtoField mnumber (ctor mtype) mname moptions Nothing

messageMapField :: Parser DotProtoField
messageMapField = do string "map"
                     whiteSpace
                     string "<"
                     whiteSpace
                     ktype <- primType
                     whiteSpace
                     string ","
                     whiteSpace
                     vtype <- primType
                     whiteSpace
                     string ">"
                     whiteSpace
                     mname <- identifier
                     whiteSpace
                     string "="
                     fpos <- fieldNumber
                     whiteSpace
                     fos <- optionAnnotation
                     whiteSpace
                     string ";"
                     -- TODO: parse comments
                     return $ DotProtoField fpos (Map ktype vtype) mname fos Nothing

--------------------------------------------------------------------------------
-- enumerations

enumField :: Parser DotProtoEnumPart
enumField = do fname <- identifier
               whiteSpace
               string "="
               whiteSpace
               fpos <- fromInteger <$> integer
               whiteSpace
               opts <- optionAnnotation
               whiteSpace
               string ";"
               return $ DotProtoEnumField fname fpos opts


enumStatement :: Parser DotProtoEnumPart
enumStatement = try (DotProtoEnumOption <$> topOption)
            <|> enumField
            <|> empty $> DotProtoEnumEmpty

enum :: Parser DotProtoDefinition
enum = do string "enum"
          whiteSpace
          ename <- Single <$> identifierName
          whiteSpace
          ebody <- braces (enumStatement `sepEndBy` whiteSpace)
          return $ DotProtoEnum ename ebody

--------------------------------------------------------------------------------
-- oneOf

oneOfField :: Parser DotProtoField
oneOfField = do ftype <- Prim <$> primType
                whiteSpace
                fname <- identifier
                whiteSpace
                string "="
                whiteSpace
                fpos <- fromInteger <$> integer
                whiteSpace
                fops <- optionAnnotation
                whiteSpace
                string ";"
                -- TODO: parse comments
                return $ DotProtoField fpos ftype fname fops Nothing

messageOneOf :: Parser DotProtoMessagePart
messageOneOf = do string "oneof"
                  whiteSpace
                  name <- identifier
                  whiteSpace
                  body <- braces $ (oneOfField <|> empty $> DotProtoEmptyField) `sepEndBy` whiteSpace
                  return $ DotProtoMessageOneOf name body

--------------------------------------------------------------------------------
-- field reservations

range :: Parser DotProtoReservedField
range = do lookAhead (integer >> whiteSpace >> string "to") -- [note] parsec commits to this parser too early without this lookahead
           s <- fromInteger <$> integer
           whiteSpace
           string "to"
           whiteSpace
           e <- fromInteger <$> integer
           return $ FieldRange s e

ranges :: Parser [DotProtoReservedField]
ranges = (try range <|> (SingleField . fromInteger <$> integer)) `sepBy1` listSep

reservedField :: Parser [DotProtoReservedField]
reservedField = do string "reserved"
                   whiteSpace
                   v <- ranges <|> ((ReservedIdentifier <$> stringLit) `sepBy1` listSep)
                   whiteSpace
                   string ";"
                   return v
