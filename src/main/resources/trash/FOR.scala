private def executeFOR_EXPR(statement: Statement): Unit = {
    val forExpr = statement.instance.asInstanceOf[FOR$EXPR]
    val varsMap: ForVariables = forExpr.computeMap({
        forExpr.caption match {
            case "SELECT" => this.dh.executeDataTable(forExpr.expr.$restore(this))
            case "PARSE" => this.dh.parseTable(forExpr.expr.$eval(this))
            case _ => null
        }
    })

    FOR_VARIABLES.push(varsMap)
    EXECUTING.push(statement)
    //根据loopMap遍历/
    while (varsMap.hasNext) {
        this.execute(statement.statements)
    }
}

private def executeFOR_TO(statement: Statement): Unit = {
    val toLoop: FOR$TO = statement.instance.asInstanceOf[FOR$TO]
    this.updateVariable(toLoop.variable, toLoop.parseBegin(this))
    EXECUTING.push(statement)
    while (toLoop.hasNext(this)) {
        this.execute(statement.statements)
        this.updateVariable(toLoop.variable, this.findVariable(toLoop.variable).value.asInstanceOf[Int] + 1)
    }
}

private def executeFOR_IN(statement: Statement): Unit = {
    val inMap: ForVariables = statement.instance.asInstanceOf[FOR$IN].computeMap(this)
    FOR_VARIABLES.push(inMap)
    EXECUTING.push(statement)
    while (inMap.hasNext) {
        this.execute(statement.statements)
    }
}