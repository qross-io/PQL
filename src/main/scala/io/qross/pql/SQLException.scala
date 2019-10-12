package io.qross.pql

class SQLParseException(val s: String) extends RuntimeException(s) {

}

class SQLExecuteException(val s: String) extends RuntimeException(s) {

}

object SharpInapplicableLinkNameException {
    def occur(linkName: String, origin: String): SharpInapplicableLinkNameException = {
        new SharpInapplicableLinkNameException(s"Inapplicable data type for link name $linkName: $origin")
    }
}

class SharpInapplicableLinkNameException(val s: String) extends RuntimeException(s) {

}

object SharpLinkArgumentException {
    def occur(linkName: String, origin: String): SharpLinkArgumentException = {
        new SharpLinkArgumentException(s"Empty or wrong argument at $linkName: $origin")
    }
}

class SharpLinkArgumentException(val s: String) extends RuntimeException(s) {

}

