package io.qross.pql

class END$IF {
    def execute(PQL: PQL): Unit = {
        //结束本次IF语句
        if (PQL.IF$BRANCHES.head) { //在IF成功时才会有语句块进行栈
            PQL.EXECUTING.pop()
        }
        PQL.IF$BRANCHES.pop()
    }
}
