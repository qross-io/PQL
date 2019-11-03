package io.qross.pql

import java.util.regex.Pattern

import scala.util.matching.Regex
import io.qross.ext.TypeExt._

object Patterns {

    val $USER$FUNCTION: Regex = """^FUNCTION\s+\$([a-z0-9_]+)\s*\(([^\)]*)\)\s*BEGIN\s""".r
    val $FUNCTION$ARGUMENT: Regex = """(?i)^\$[a-z0-9]+(\s+[a-z]+)?(\s+DEFAULT\s+.+)?$""".r

    val $IF: Pattern = Pattern.compile("""^IF\s+([\s\S]+?)\s+THEN\s""", Pattern.CASE_INSENSITIVE)
    val $IF$: Regex = """(?i)^IF\s""".r
    val $ELSIF: Pattern = Pattern.compile("""^ELSIF\s+([\s\S]+?)\s+THEN\s""", Pattern.CASE_INSENSITIVE)
    val $ELSE: Regex = """(?i)^ELSE\s""".r
    val $END$IF: Regex = """(?i)^END\s+IF$""".r

    val $WHILE: Pattern = Pattern.compile("""^WHILE\s+([\s\S]+)\s+LOOP""", Pattern.CASE_INSENSITIVE)
    val $END$LOOP: Regex = """(?i)^END\s+LOOP$""".r

    val $BLANK: Regex = """\s""".r
    val BLANKS: String = """\s+"""
    val $CAPTION: Regex = """(?i)^\b[a-z]+\b(\s+\b[a-z]+\b)+""".r
    val $PHRASE: Regex = """(?i)\b[a-z]+\b(\s+\b[a-z]+\b)+""".r

    val $BRACKET: Pattern = Pattern.compile("""\(([^\)]+)\)""", Pattern.CASE_INSENSITIVE)
    val $AND$: Pattern = Pattern.compile("""(^|\sOR\s)(([\s\S]+?)\s+AND\s+([\s\S]+?))($|\sAND|\sOR)""", Pattern.CASE_INSENSITIVE)
    val $OR: Pattern = Pattern.compile("\\sOR\\s", Pattern.CASE_INSENSITIVE)
    val $OR$: Pattern = Pattern.compile("""(^)(([\s\S]+?)\s+OR\s+([\s\S]+?))(\sOR|$)""", Pattern.CASE_INSENSITIVE)
    val A$AND$Z: Regex = """(?i)[A-Z]{3}\s+AND\s+[A-Z]{3}""".r
    val A$OR$Z: Regex = """(?i)[A-Z]{3}\s+OR\s+[A-Z]{3}""".r

    val $SELECT$: Pattern = Pattern.compile("""\(\s*SELECT\s""", Pattern.CASE_INSENSITIVE)  //(SELECT...)
    val $CONDITION: Regex = """(?i)^~condition\[(\d+)\]$""".r

    val STATEMENTS: Set[String] = Set[String]("IF", "ELSIF", "ELSE", "FOR", "WHILE", "CALL")
    val NON_QUERY_CAPTIONS: Set[String] = Set[String]("INSERT", "UPDATE", "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT")
    val $SELECT: Regex = """(?i)^SELECT\s""".r
    val $DELETE: Regex = """(?i)^DELETE\s""".r
    val $DELETE$FILE: Regex = """(?i)^DELETE\s+FILE\s""".r
    val $NON_QUERY: Regex = s"(?i)^(${NON_QUERY_CAPTIONS.mkString("|")})\\s".r

    val $FOR: Pattern = Pattern.compile("""^FOR\s+([\s\S]+?)\s+IN\s+([\s\S]+?)\s+LOOP""", Pattern.CASE_INSENSITIVE)
    val $EXIT: Pattern = Pattern.compile("""^EXIT(\s+WHEN\s([\s\S]+))?$""", Pattern.CASE_INSENSITIVE)
    val $EXIT$CODE: Regex = """(?i)^EXIT\s+(CODE|PROCEDURE)(\s|$)""".r
    val $CONTINUE: Pattern = Pattern.compile("""^CONTINUE(\s+WHEN\s([\s\S]+))?$""", Pattern.CASE_INSENSITIVE)
    val $SET: Regex = """(?i)^SET\s+([\s\S]+?)(:=|=:)([\s\S]+)$""".r
    val $VAR: Regex = """(?i)^VAR\s+\$[a-z0-9_]""".r
    val $DECLARE: Regex = """(?i)^DECLARE\s+\$[a-z0-9_]""".r
    val $DATA_TYPE: Regex = """(?i)^(INT|INTEGER|DECIMAL|BOOLEAN|TEXT|DATETIME|MAP|OBJECT|ROW|TABLE|ARRAY|LIST|JSON|REGEX)$""".r
    val $OPEN: Regex = """(?i)^OPEN\s+""".r
    val $USE: Regex = """(?i)^USE\s+""".r
    val $SAVE$AS: Regex = """(?i)^SAVE\s+AS\s+""".r
    val $CACHE: Regex = """(?i)^CACHE\s+(\S+)\s*#""".r
    val $TEMP: Regex = """(?i)^TEMP\s+(\S+)\s*#""".r
    val PRIMARY$KEY: Regex = """(?i)\sPRIMARY(\s+KEY)?\s""".r
    val $GET: Regex = """(?i)^GET\s*#""".r
    val $PASS: Regex = """(?i)^PASS\s*#""".r
    val $PUT: Regex = """(?i)^PUT\s*#""".r
    val $PREP: Regex = """(?i)^PREP\s*#""".r
    val $OUTPUT: Regex = """(?i)^OUTPUT\s*#?""".r
    val $PRINT: Regex = """(?i)^PRINT\s""".r
    val $PRINT$SEAL: Regex = """(?i)^[a-z]+\s""".r
    val $SHOW: Pattern = Pattern.compile("""^SHOW\s+(\d+)""", Pattern.CASE_INSENSITIVE)
    val $RUN: Regex = """(?i)^RUN\s+(COMMAND|SHELL)\s+""".r
    val $REQUEST: Regex = """(?i)^REQUEST\s+JSON\s+API\s+""".r
    val $REQUEST$METHOD: Pattern = Pattern.compile("""\s+(POST|PUT|DELETE)(\s+(\S+))?""", Pattern.CASE_INSENSITIVE)
    val $REQUEST$HEADER: Pattern = Pattern.compile("""\s+SET\s+HEADER\s+(\S+)""", Pattern.CASE_INSENSITIVE)
    val $SEND$MAIL: Regex = """(?i)^SEND\s+E?MAIL\s+""".r
    val $PARSE: Regex = """(?i)^PARSE\s""".r
    val $AS: Regex = """(?i)\sAS\s""".r
    val $LINK: Regex = """(?i)\s[A-Z][A-Z\$]+([ \t]+[A-Z\$]+[A-Z])*(\s|$)|\s%\s""".r
    val $LET: Regex = """(?i)^LET\s""".r
    val $DEBUG: Regex = """(?i)^DEBUG\s""".r
    val $ECHO: Regex = """(?i)^ECHO(\s|$)""".r
    val $SLEEP: Regex = """(?i)^SLEEP\s""".r
    val $CALL: Regex = """(?i)^CALL\s""".r
    val $RETURN: Regex = """(?i)^RETURN(\s|$)""".r

    val $VARIABLE: Regex = """^(\$|@)\(?[A-Za-z0-9_]+\)?$""".r
    val $INTERMEDIATE$N: Regex = """^~value\[(\d+)\]$""".r
    val FUNCTION_NAMES: Set[String] = GlobalFunction.getClass.getDeclaredMethods.map(m => m.getName).filter(n => "^[A-Z]".r.test(n)).toSet
    val $RESERVED: Regex = """^[_A-Za-z0-9\.]+$""".r
    val $CONSTANT: Regex = """^[A-Za-z_][A-Za-z0-9_]+$""".r

    val SHARP_LINKS: Set[String] = Sharp.getClass.getDeclaredMethods.map(m => m.getName).filter(n => """^[A-Z][A-Z\$]*[A-Z]$""".r.test(n)).toSet
    val MULTI$ARGS$LINKS: Map[String, Set[String]] = Map[String, Set[String]](
        "SUBSTRING" -> Set[String]("TO"),
        "SPLIT" -> Set[String]("AND"),
        "REPLACE" -> Set[String]("TO"),
        "REPLACE$FIRST" -> Set[String]("TO"),
        "REPLACE$ALL" -> Set[String]("TO"),
        "SUBSTRING" -> Set[String]("TO"),
        "PLUS" -> Set[String]("YEAR", "YEARS", "MONTH", "MONTHS", "DAY", "DAYS", "HOUR", "HOURS", "MINUTE", "MINUTES", "SECOND", "SECONDS", "MILLI", "MILLIS", "MILLISECONDS"),
        "MINUS" -> Set[String]("YEAR", "YEARS", "MONTH", "MONTHS", "DAY", "DAYS", "HOUR", "HOURS", "MINUTE", "MINUTES", "SECOND", "SECONDS", "MILLI", "MILLIS", "MILLISECONDS"))
    val $DATETIME_UNITS: Regex = """^(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND|MILLI|MICRO|NONA)=(\d+)$""".r
    val GLOBAL_VARIABLES: Set[String] = GlobalVariableDeclaration.getClass.getDeclaredMethods.map(m => m.getName).toSet
    val ARROW: String = "->"

    val EMBEDDED: String = "EMBEDDED:"
    val EM$LEFT: String = "<%"
    val EM$RIGHT: String = "%>"
}
