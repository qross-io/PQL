package io.qross.fql

import scala.util.matching.Regex

object Patterns {

    val $SELECT: Regex = """(?i)^SELECT\s([\s\S]+?)\s(FROM\b)""".r
    val $FROM: Regex = """(?i)^FROM\s([\s\S]+?)(\bWHEN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\sLIMIT\b|$)""".r
    val $SEEK: Regex = """(?i)\sSEEK\s([\s\S]+)(\bINNER\b|\bOUTER\b|\bLEFT\b|\bRIGHT\b|\bON\b|$)""".r
    val $WHERE: Regex = """(?i)^WHERE\s([\s\S]+?)(\bGROUP\b|\bORDER\b|\bLIMIT\b|$)""".r
    val $GROUP$BY: Regex = """(?i)^GROUP\s+BY\s([\s\S]+?)(\bHAVING\b|\bORDER\b|\bLIMIT\b|$)""".r
    val $HAVING: Regex = """(?i)^HAVING\s([\s\S]+?)(\bORDER\b|\bLIMIT\b|$)""".r
    val $ORDER$BY: Regex = """(?i)^ORDER\s+BY\s([\s\S]+?)(\bLIMIT\b|$)""".r
    val $LIMIT: Regex = """(?i)^LIMIT\s([\s\S]+?)$""".r

    val $INTO: Regex = """""".r
    val $VALUES: Regex = """VALUES|SELECT""".r

    val $BLANK: Regex = """\s""".r
    val BLANKS: String = """\s+"""

    val $DISTINCT: Regex = """(?i)^DISTINCT\s""".r
    val $FUNCTION: Regex = """(?i)[A-Z][A-Z_]+[A-Z]\s*\([^\(\)]+\)""".r
    val $QUERY: Regex = """(?i)(SELECT\s[^\)]+)""".r //子查询

    val $BRACKET: Regex = """\(([^\(\)]+?)\)""".r

    val $AND$: Regex = """(^|\sOR\s)(([\s\S]+?)\s+AND\s+([\s\S]+?))($|\sAND|\sOR)""".r
    val $OR: Regex = """\sOR\s""".r
    val $OR$: Regex = """(^)(([\s\S]+?)\s+OR\s+([\s\S]+?))(\sOR|$)""".r
    val $CONDITION$N: Regex = """(?i)^~condition\[(\d+)\]$""".r

    val IN$$: Regex = """(?i)\sIN\s*\([^\(\)]*\)""".r  // IN (...)
}