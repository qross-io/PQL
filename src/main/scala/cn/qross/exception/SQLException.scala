package cn.qross.exception

class SQLParseException(val s: String) extends RuntimeException(s) {

}

class SQLExecuteException(val s: String) extends RuntimeException(s) {

}

class ClassMethodNotFoundException(val s: String) extends RuntimeException(s) {

}

class TableColumnNotFoundException(val s: String) extends RuntimeException(s) {

}

class IncorrectFunctionNameException(val s: String) extends RuntimeException(s) {

}

class MissingColumnAliasNameException(val s: String) extends RuntimeException(s) {

}

class OutOfIndexBoundaryException(val s: String) extends RuntimeException(s) {

}

class IncorrectPropertyNameException(val s: String) extends RuntimeException(s) {

}

class IncorrectIndexDataTypeException(val s: String) extends RuntimeException(s) {

}

class UnsupportedDataTypeException(val s: String) extends RuntimeException(s) {

}

class EmailInvalidSenderException(val s: String) extends RuntimeException(s) {

}

class UnsupportedSentenceException(val s: String) extends RuntimeException(s) {

}

object SharpInapplicableLinkNameException {
    def occur(linkName: String, origin: String): SharpInapplicableLinkNameException = {
        new SharpInapplicableLinkNameException(s"Inapplicable data type for sharp link $linkName: $origin")
    }
}

class SharpInapplicableLinkNameException(val s: String) extends RuntimeException(s) {

}

object SharpLinkArgumentException {
    def occur(linkName: String, origin: String): SharpLinkArgumentException = {
        new SharpLinkArgumentException(s"Empty or incorrect argument(s) at $linkName: $origin")
    }

    def miss(linkName: String, origin: String, expected: Int, actual: Int): SharpLinkArgumentException = {
        new SharpLinkArgumentException(s"Missing argument(s) at $linkName: $origin. expected: $expected, actual: $actual")
    }
}

class SharpLinkArgumentException(val s: String) extends RuntimeException(s) {

}

class SharpDataExceptionException(val s: String) extends RuntimeException(s) {

}