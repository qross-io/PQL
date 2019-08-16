import io.qross.core.{DataRow, DataTable}
import io.qross.sql.Patterns.{$BLANK, $PARSE}

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

仅为了遍历单行row意义不大
else if (collection.bracketsWith("$", "}}")) {
    //                        $list无意义
    //                        $table无意义
    //                        $value不能遍历
    //                        $row有意义
    var sentence = collection.$trim("$", "}}")
    val resultType = sentence.takeBefore("{{").toUpperCase().trim()
    sentence = sentence.takeAfter("{{")
    val caption = sentence.takeBefore($BLANK).toUpperCase()

    val table = if (caption == "SELECT") {
        PSQL.dh.executeDataTable(sentence.$restore(PSQL))
    }
    else if (caption == "PARSE") {
        PSQL.dh.parseTable(sentence.takeAfter($PARSE).$eval(PSQL).asText)
    }
    else {
        DataTable()
    }

    if (Set("ROW", "OBJECT", "MAP").contains(resultType)) {
        table.firstRow.getOrElse(DataRow()).toTable()
    }
    else {
        table
    }
}