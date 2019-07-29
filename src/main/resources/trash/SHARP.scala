class SHARP1(val expression: String) {

    private val sections = new mutable.ListBuffer[String]()
    private var section = ""

    expression.takeAfter("""(?i)^LET\s""".r)
        .trim()
        .split("")
        .foreach(char => {
            if (section == "") {
                section += char
            }
            else if ($BLANK.test(char)) {
                if ($BLANK.test(section)) {
                    //保存新的节
                    sections += section
                    section = char
                }
                else {
                    //多个空字符
                    section += char
                }
            }
            else {
                if ($BLANK.test(section)) {
                    section += char
                }
                else {
                    sections += section
                    section = char
                }
            }
        })

    if (section != "") {
        sections += section
    }

    for (i <- sections.indices) {
        if ("""^\s+$""".r.test(sections(i))) {
            //保留字
            if (SHARP_RESERVED.contains(sections(i-1))) {
                //保留字
                if (SHARP_RESERVED.contains(sections(i+1))) {
                    sections(i-1) = "#>" + sections(i-1) + "$" + sections(i+1)
                    sections(i+1) = ""
                }
                else {
                    sections(i-1) = "#>" + sections(i-1)
                }
            }
            //常量
            else if (sections(i-1) != "") {
                //常量
                if (!SHARP_RESERVED.contains(sections(i+1))) {
                    sections(i+1) = sections(i-1) + sections(i) + sections(i+1)
                    sections(i-1) = ""
                }
            }
            sections(i) = ""
        }
    }

    def execute(): Any = {
        val all = sections.filter(_ != "")
        var value: AnyRef = all.head

        for (i <- all.indices) {
            //单数是保留字指令
            if (all(i).startsWith("#>")) {
                value = SHARP.getClass.getDeclaredMethod(all(i).substring(2)).invoke(null, value, if (i + 1 < all.size) all(i+1) else null)
                //value = SHARP.EXECUTOR(all(i).substring(2))(value, if (i + 1 < all.size) all(i+1) else null)
            }
        }
        value
    }
}
