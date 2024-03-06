package cn.qross.pql

class END$LOOP {
    def execute(PQL: PQL): Unit = {
        //除了FOR以外还是WHILE循环
        if (PQL.EXECUTING.head.caption == "FOR") {
            PQL.FOR$VARIABLES.pop()
        }
        PQL.EXECUTING.pop()
        //重置break变量
        if (PQL.breakCurrentLoop) {
            PQL.breakCurrentLoop = false
        }
    }

}
