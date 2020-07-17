package io.qross.pql

import java.util.regex.Pattern

import scala.util.matching.Regex
import io.qross.ext.TypeExt._

object Patterns {

    val $USER$FUNCTION: Regex = """^FUNCTION\s+\$([a-z0-9_]+)\s*\(([^\)]*)\)\s*BEGIN\s""".r
    val $FUNCTION$ARGUMENT: Regex = """(?i)^\$[a-z0-9]+(\s+[a-z]+)?(\s+DEFAULT\s+.+)?$""".r

    val $IF: Regex = """(?i)^IF\s+([\s\S]+?)\s+THEN\b""".r
    val $ELSIF: Regex = """(?i)^ELSIF\s+([\s\S]+?)\s+THEN\b""".r
    val $ELSE: Regex = """(?i)^ELSE\b""".r
    val $END$IF: Regex = """(?i)^END\s+IF$""".r

    val $WHILE: Pattern = Pattern.compile("""^WHILE\s+([\s\S]+)\s+LOOP\b""", Pattern.CASE_INSENSITIVE)
    val $END$LOOP: Regex = """(?i)^END\s+LOOP$""".r

    val $CASE: Regex = """(?i)^CASE\b""".r
    val $WHEN: Regex = """(?i)^WHEN\s+([\s\S]+?)\s+THEN\b""".r
    val $END$CASE: Regex = """(?i)^END\s+CASE$""".r
    val $WHEN$: Regex = """(?i)\sWHEN\s""".r

    val $CONDITION: Regex = """(?i)=|!=|>|<|IS""".r

    val $END$: Regex = """(?i)\bEND\b""".r

    val $BLANK: Regex = """\s""".r
    val BLANKS: String = """\s+"""
    val $PHRASE: Regex = """(?i)^\b[a-z]+(\s+[a-z]+)*\b""".r

    val $BRACKET: Regex = """\(([^\(\)]+?)\)""".r
    val $AND$: Pattern = Pattern.compile("""(^|\sOR\s)(([\s\S]+?)\s+AND\s+([\s\S]+?))($|\sAND|\sOR)""", Pattern.CASE_INSENSITIVE)
    val $OR: Pattern = Pattern.compile("\\sOR\\s", Pattern.CASE_INSENSITIVE)
    val $OR$: Pattern = Pattern.compile("""(^)(([\s\S]+?)\s+OR\s+([\s\S]+?))(\sOR|$)""", Pattern.CASE_INSENSITIVE)
    val A$AND$Z: Regex = """(?i)[A-Z]{3}\s+AND\s+[A-Z]{3}""".r
    val A$OR$Z: Regex = """(?i)[A-Z]{3}\s+OR\s+[A-Z]{3}""".r

    val $SELECT$: Regex = """(?i)\(\s*SELECT\s""".r  //(SELECT...)
    val $CONDITION$N: Regex = """(?i)^~condition\[(\d+)\]$""".r
    val IN$$: Regex = """(?i)\sIN\s*\([^\(\)]*\)""".r  // IN (...)

    val STATEMENTS: Set[String] = Set[String]("IF", "ELSIF", "ELSE", "FOR", "WHILE", "CALL", "CASE", "WHEN")
    val NON_QUERY_CAPTIONS: Set[String] = Set[String]("INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT")
    val $INSERT$INTO: Regex = """(?i)^INSERT\s+INTO\s""".r
    //val $DELETE: Regex = """(?i)^DELETE\s""".r
    //val $VALUES: Regex = """(?i)\bVALUES\b""".r
    val $SELECT: Regex = """(?i)^SELECT\s""".r
    val $NON$QUERY: Regex = s"(?i)^(${NON_QUERY_CAPTIONS.mkString("|")})\\s".r

    //自定义SQL语句
    val $NONE$QUERY$CUSTOM: Regex = """(?i)^(INSERT\s+INTO|UPDATE|DELETE|DELETE\s+FROM)\s+(\S+?:|:|\$|@)\S+\s""".r
    val $SELECT$CUSTOM: Regex = """(?)\b(FROM|JOIN)\s+(\S+?:|:|\$|@)\S+\b""".r

    val $FOR: Regex = """(?i)^FOR\s+([\s\S]+?)\s+(IN|OF)\s+([\s\S]+?)\s+LOOP\b""".r
    val $EXIT: Regex = """(?i)^EXIT(\s+WHEN\s([\s\S]+))?$""".r
    val $EXIT$CODE: Regex = """(?i)^EXIT\s+(CODE|PROCEDURE)\b""".r
    val $CONTINUE: Regex = """(?i)^CONTINUE(\s+WHEN\s([\s\S]+))?$""".r
    val $SET: Regex = """(?i)^SET\s+([\s\S]+?)(:=|=:)([\s\S]+)$""".r
    val $VAR: Regex = """(?i)^VAR\s+\$[a-z0-9_]""".r
    val $DECLARE: Regex = """(?i)^DECLARE\s+\$[a-z0-9_]""".r
    val $DATA_TYPE: Regex = """(?i)^(INT|INTEGER|DECIMAL|BOOLEAN|TEXT|DATETIME|MAP|OBJECT|ROW|TABLE|ARRAY|LIST|JSON|REGEX)$""".r
    val $OPEN: Regex = """(?i)^OPEN\s+""".r
    val $USE: Regex = """(?i)^USE\s+""".r
    val $SAVE: Regex = """(?i)^SAVE\s+(AS|TO)\s""".r
    val $CACHE: Regex = """(?i)^CACHE\s+([\s\S]+)#""".r
    val $TEMP: Regex = """(?i)^TEMP\s+([\s\S]+)#""".r
    val $GET: Regex = """(?i)^GET\s*#""".r
    val $PASS: Regex = """(?i)^PASS\s*#""".r
    val $PUT: Regex = """(?i)^PUT\s*#""".r
    val $PREP: Regex = """(?i)^PREP\s*#""".r
    val $PAGE: Regex = """(?i)^PAGE\s*#\s*SELECT\b""".r
    val $BLOCK: Regex = """(?i)^BLOCK[\s\S]+?#\s*(SELECT|UPDATE|DELETE)\b""".r
    val $PROCESS: Regex = """(?i)^PROCESS\s*#\s*SELECT\b""".r
    val $BATCH: Regex = """(?i)^BATCH\s*#""".r
    val $OUTPUT: Regex = """(?i)^OUTPUT\s*#?""".r
    val $PRINT: Regex = """(?i)^PRINT\s""".r
    val $PRINT$SEAL: Regex = """(?i)^[a-z]+\s""".r
    val $SHOW: Regex = """(?i)^SHOW\s+(\d+)$""".r
    val $RUN: Regex = """(?i)^RUN\s+(COMMAND|SHELL)\s+""".r
    val $REQUEST: Regex = """(?i)^REQUEST\s+""".r
    val $SEND: Regex = """(?i)^SEND\s+""".r
    val $PARSE: Regex = """(?i)^PARSE\s""".r
    val $INVOKE: Regex = """(?i)^INVOKE\s""".r
    
    val $FILE: Regex = """(?i)^FILE\s""".r
    val $DIR: Regex = """(?i)^DIR\s""".r

    val $LINK: Regex = """(?i)\s[A-Z][A-Z]+(\s+[A-Z][A-Z\d]+)*\b""".r
    val $ARGS: Regex = """\s+[,=]\s+|\s+[,=\)]|[,=\(]\s+""".r
    val $AS: Regex = """(?i)\s+AS\s+""".r

    val $LET: Regex = """(?i)^LET\s+""".r
    val $DEBUG: Regex = """(?i)^DEBUG\s""".r
    val $ECHO: Regex = """(?i)^ECHO\b""".r
    val $SLEEP: Regex = """(?i)^SLEEP\s""".r
    val $CALL: Regex = """(?i)^CALL\s""".r
    val $RETURN: Regex = """(?i)^RETURN\b""".r
    val $EXEC: Regex = """(?i)EXEC\s""".r

    val $VARIABLE: Regex = """^(\$|@)\(?[A-Za-z0-9_]+\)?$""".r
    val $INTERMEDIATE$N: Regex = """^~value\[(\d+)\]$""".r
    val FUNCTION_NAMES: Set[String] = GlobalFunction.getClass.getDeclaredMethods.map(m => m.getName).filter(n => "^[A-Z]".r.test(n)).toSet
    val $RESERVED: Regex = """^[_A-Za-z0-9\.]+$""".r
    val $CONSTANT: Regex = """^\*$|^[A-Za-z][A-Za-z0-9_]*$|^[_][A-Za-z0-9_]+$""".r
    val $NULL: Regex = """(?i)^NULL$""".r

    val $DATETIME_UNITS: Regex = """^(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND|MILLI|MICRO|NONA)=(\d+)$""".r
    val GLOBAL_VARIABLES: Set[String] = GlobalVariableDeclaration.getClass.getDeclaredMethods.map(m => m.getName).toSet
    val ARROW: String = "->"

    val EMBEDDED: String = "EMBEDDED:"
    val EM$LEFT: String = "<%"
    val EM$RIGHT: String = "%>"

    val $INTEGER: Regex = """^-?\d+$""".r
    val $DECIMAL: Regex = """^-?\d+\.\d+$""".r
}
