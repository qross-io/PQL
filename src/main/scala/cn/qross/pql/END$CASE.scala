package cn.qross.pql

class END$CASE {
    def execute(PQL: PQL): Unit = {
        //WHEN成功时才进栈
        if (PQL.CASE$WHEN.head.matched) {
            PQL.EXECUTING.pop() //退出WHEN或ELSE
        }
        PQL.EXECUTING.pop() //退出CASE
        PQL.CASE$WHEN.pop() //退出equivalent
    }
}
