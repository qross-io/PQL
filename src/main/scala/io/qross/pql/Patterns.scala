package io.qross.pql

import java.util.regex.Pattern

import scala.util.matching.Regex
import io.qross.ext.TypeExt._

object Patterns {

    val $IF: Pattern = Pattern.compile("""^IF\s+([\s\S]+?)\s+THEN""", Pattern.CASE_INSENSITIVE)
    val $ELSE_IF: Pattern = Pattern.compile("""^ELS(E )?IF\s+([\s\S]+?)\s+THEN""", Pattern.CASE_INSENSITIVE)
    val $ELSE: Pattern = Pattern.compile("^ELSE", Pattern.CASE_INSENSITIVE)
    val $END_IF: Pattern = Pattern.compile("""^END\s+IF""", Pattern.CASE_INSENSITIVE)

    val $WHILE: Pattern = Pattern.compile("""^WHILE\s+([\s\S]+)\s+LOOP""", Pattern.CASE_INSENSITIVE)
    val $END_LOOP: Pattern = Pattern.compile("""^END\s+LOOP""", Pattern.CASE_INSENSITIVE)

    val $BLANK: Regex = """\s""".r
    val BLANKS: String = """\s+"""

    val $BRACKET: Pattern = Pattern.compile("""\(([^\)]+)\)""", Pattern.CASE_INSENSITIVE)
    val $AND: Pattern = Pattern.compile("""(^|\sOR\s)(([\s\S]+?)\s+AND\s+([\s\S]+?))($|\sAND|\sOR)""", Pattern.CASE_INSENSITIVE)
    val $_OR: Pattern = Pattern.compile("\\sOR\\s", Pattern.CASE_INSENSITIVE)
    val $OR: Pattern = Pattern.compile("""(^)(([\s\S]+?)\s+OR\s+([\s\S]+?))(\sOR|$)""", Pattern.CASE_INSENSITIVE)

    val $SELECT$: Pattern = Pattern.compile("""\(\s*SELECT\s""", Pattern.CASE_INSENSITIVE)  //(SELECT...)
    val $CONDITION: Regex = """(?i)^~condition\[(\d+)\]$""".r

    val NON_QUERY_CAPTIONS: Set[String] = Set[String]("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE")
    val $SELECT: Regex = """(?i)^SELECT\s""".r
    val $NON_QUERY: Regex = s"(?i)^(${NON_QUERY_CAPTIONS.mkString("|")})\\s".r

    val $FOR: Pattern = Pattern.compile("""^FOR\s+([\s\S]+?)\s+IN\s+([\s\S]+?)\s+LOOP""", Pattern.CASE_INSENSITIVE)
    val $EXIT: Pattern = Pattern.compile("""^EXIT(\s+WHEN\s([\s\S]+))?$""", Pattern.CASE_INSENSITIVE)
    val $CONTINUE: Pattern = Pattern.compile("""^CONTINUE(\s+WHEN\s([\s\S]+))?$""", Pattern.CASE_INSENSITIVE)
    val $SET: Pattern = Pattern.compile("""^SET\s+([\s\S]+?):=([\s\S]+)$""", Pattern.CASE_INSENSITIVE)
    val $DATA_TYPE: Regex = """(?i)^(INT|INTEGER|DECIMAL|BOOLEAN|TEXT|DATETIME|MAP|OBJECT|ROW|TABLE|ARRAY|LIST|JSON|REGEX)$""".r
    val $OPEN: Pattern = Pattern.compile("""^OPEN\s+([\s\S]+)$""", Pattern.CASE_INSENSITIVE)
    val $USE: Pattern = Pattern.compile("""(?i)^USE\s+""")
    val $SAVE$AS: Pattern = Pattern.compile("""^SAVE\s+AS\s+([\s\S]+)$""", Pattern.CASE_INSENSITIVE)
    val $CACHE: Pattern = Pattern.compile("""^CACHE\s+(\S+)\s*#""", Pattern.CASE_INSENSITIVE)
    val $TEMP: Pattern = Pattern.compile("""^TEMP\s+(\S+)\s*#""", Pattern.CASE_INSENSITIVE)
    val $GET: Pattern = Pattern.compile("""^GET\s*#""", Pattern.CASE_INSENSITIVE)
    val $PASS: Pattern = Pattern.compile("""^PASS\s*#""", Pattern.CASE_INSENSITIVE)
    val $PUT: Pattern = Pattern.compile("""^PUT\s*#""", Pattern.CASE_INSENSITIVE)
    val $PREP: Pattern = Pattern.compile("""^PREP\s*#""", Pattern.CASE_INSENSITIVE)
    val $OUTPUT: Regex = """(?i)^OUTPUT\s*#?""".r
    val $PRINT: Pattern = Pattern.compile("""^PRINT\s+?([a-z]+\s+)?([\s\S]+)$""", Pattern.CASE_INSENSITIVE)
    val $SHOW: Pattern = Pattern.compile("""^SHOW\s+(\d+)""", Pattern.CASE_INSENSITIVE)
    val $RUN: Pattern = Pattern.compile("""^RUN\s+COMMAND\s+""", Pattern.CASE_INSENSITIVE)
    val $REQUEST: Pattern = Pattern.compile("""^REQUEST\s+JSON\s+API\s+""", Pattern.CASE_INSENSITIVE)
    val $REQUEST$METHOD: Pattern = Pattern.compile("""\s+(POST|PUT|DELETE)(\s+(\S+))?""", Pattern.CASE_INSENSITIVE)
    val $REQUEST$HEADER: Pattern = Pattern.compile("""\s+SET\s+HEADER\s+(\S+)""", Pattern.CASE_INSENSITIVE)
    val $SEND$MAIL = """(?i)^SEND\s+MAIL\s+""".r
    val $PARSE: Regex = """(?i)^PARSE\s+""".r
    val $LINK: Regex = """(?i)\s[a-z]+(\s+[a-z]+)*(\s|$)""".r
    val $LET: Regex = """(?i)^LET\s""".r
    val $DEBUG: Regex = """(?i)^DEBUG\s""".r
    val $ECHO: Regex = """(?i)^ECHO(\s|$)""".r
    val $SLEEP: Regex = """(?i)^SLEEP\s""".r

    val $VARIABLE: Regex = """^(\$|@)\(?[A-Za-z0-9_]+\)?$""".r
    val $INTERMEDIATE$N: Regex = """^~value\[(\d+)\]$""".r
    val FUNCTION_NAMES: Set[String] = Function.getClass.getDeclaredMethods.map(m => m.getName).filter(n => "^[A-Z]".r.test(n)).toSet
    val $RESERVED: Regex = """^[_A-Za-z0-9\.]+$""".r
    val $CONSTANT: Regex = """^[A-Za-z_][A-Za-z0-9_]+$""".r

    val SHARP_LINKS: Set[String] = SHARP.getClass.getDeclaredMethods.map(m => m.getName).filter(n => """^[A-Z][A-Z\$]*[A-Z]$""".r.test(n)).toSet
    val ARROW: String = "->"

    val EMBEDDED: String = "EMBEDDED:"
    val EM$LEFT: String = "<%"
    val EM$RIGHT: String = "%>"
}
