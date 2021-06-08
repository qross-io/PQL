package io.qross.pql.test

import java.net.{URLDecoder, URLEncoder}

import io.qross.core.DataHub
import io.qross.net.Json
import io.qross.pql.PQL
import io.qross.script.Shell.{destroy, end, kill, ps$ef}
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._
import io.qross.fs.ResourceFile
import io.qross.jdbc.DataSource

object FQL {
    def main(args: Array[String]): Unit = {


        //new SELECT("select * from :hello where name='Tom' and age>18 order by age desc limit 10")

        //PQL.openFile("/sql/test.sql").recognizeParameters().iterator().forEachRemaining(println)

//        Marker.openFile("/templates/markdown.md")
//            .transform()
//            .getContent
//            .print

        //new Marker(ResourceFile.open("/templates/markdown.md").content).transform().getContent.print

        val sentence = """-- 参数列表
                         |-- 连接 connection_name
                         |-- 库 database_name
                         |-- 表 table_name
                         |-- 字段 column_name  是否数字列
                         |-- 字段类型 column_type
                         |-- 比较规则 operator  等于 = / 不等于 != / 大于 > / 大于等于 >= /小于 < / 小于等于 <= / 在字典 IN / 不在字典内 NOT IN /  / 区间 ><
                         |-- 比较值 comparison_value    NULL/EMPTY/0
                         |-- 分区字段名 partition_name
                         |-- 分组表达式 partition_expression 例如 yyyyMMdd
                         |-- 自定义条件 condition
                         |-- 自定义检查语句 sentence
                         |
                         |-- 配置错误数据的规则
                         |OPEN #{connection_name};
                         |	IF $sentence IS UNDEFINED THEN
                         |		SET $where := '';
                         |		IF $column_name IS NOT UNDEFINED AND $column_name != '' THEN
                         |			IF $comparison_value == 'NULL' THEN
                         |				IF $operator == '=' THEN
                         |					SET $where := '#{column_name} IS NULL';
                         |				ELSE
                         |					SET $where := '#{column_name} IS NOT NULL';
                         |				END IF;
                         |			ELSIF $operator IN ('>', '>=', '<', '<=') THEN
                         |				IF $column_type == 'NUMBER' THEN
                         |					SET $where := '#{column_name}#{operator}#{comparison_value}';
                         |				ELSE
                         |					SET $where := "#{column_name}#{operator}'#{comparison_value}'";
                         |				END IF;
                         |			ELSIF $operator == 'NOT IN' THEN
                         |				IF $column_type == 'NUMBER' THEN
                         |					SET $where := '''#{column_name} NOT IN (${ $comparison_value REPLACE '，' TO ',' })''';
                         |				ELSE
                         |					SET $where := '''#{column_name} NOT IN (${ $comparison_value REPLACE '，' TO ',' REPLACE ',' TO "','" })''';
                         |				END IF;
                         |			ELSIF $operator == 'IN' THEN
                         |				IF $column_type == 'NUMBER' THEN
                         |					SET $where := '''#{column_name} IN (${ $comparison_value REPLACE '，' TO ',' })''';
                         |				ELSE
                         |					SET $where := '''#{column_name} IN (${ $comparison_value REPLACE '，' TO ',' REPLACE ',' TO "','" })''';
                         |				END IF;
                         |			ELSIF $operator == '><' THEN
                         |				IF $column_type == 'NUMBER' THEN
                         |					SET $where := '''(#{column_name}#{greater_operator}#{greater_value} AND #{column_name}#{less_operator}#{less_value})''';
                         |				ELSE
                         |					SET $where := '''(#{column_name}#{greater_operator}'#{greater_value}' AND #{column_name}#{less_operator}'#{less_value})'''';
                         |				END IF;
                         |			ELSIF $comparison_value == 'EMPTY' THEN
                         |				SET $where := "#{column_name}#{operator}''";
                         |			ELSE
                         |				IF $column_type == 'NUMBER' THEN
                         |					SET $where := '#{column_name}#{operator}#{comparison_value}';
                         |				ELSE
                         |					SET $where := "#{column_name}#{operator}'#{comparison_value}'";
                         |				END IF;
                         |			END IF;
                         |		END IF;
                         |
                         |		-- 设置了附加查询条件
                         |		IF $condition != '' THEN
                         |			IF $where != '' THEN
                         |				SET $where := '''(#{condition}) AND $where''';
                         |			ELSE
                         |				SET $where := '''(#{condition})''';
                         |			END IF;
                         |		END IF;
                         |
                         |		-- 存在分区
                         |		IF $partition_name != '' THEN
                         |			IF $where != '' THEN
                         |				SET $where := '''#{partition_name}='${ $task_time FORMAT $partition_expression }' AND $where''';
                         |			ELSE
                         |				SET $wehre := '''#{partition_name}='${ $task_time FORMAT $partition_expression }'''';
                         |			END IF;
                         |		END IF;
                         |
                         |		IF $where != '' THEN
                         |			GET # SELECT COUNT(0) AS amount FROM #{database_name}.#{table_name} WHERE $where!;
                         |		ELSE
                         |			GET # SELECT COUNT(0) AS amount FROM #{database_name}.#{table_name};
                         |		END IF;
                         |	ELSE
                         |		GET # #{sentence};
                         |	END IF;
                         |
                         |SET $now := @NOW FORMAT 'yyyy-MM-dd HH:mm:ss';
                         |-- 保存结果报告
                         |SAVE TO DEFAULT;
                         |	PUT # INSERT INTO datago_qualities_reports (job_id, command_id, item, amount, report_date, create_time) VALUES ($job_id, $command_id, '#{column_name}', ?, ${ $task_time FORMAT 'yyyy-MM-dd' }, $now);
                         |
                         |-- 检查结果是否符合预期
                         |OPEN DEFAULT;
                         |SET $report_id, $amount := SELECT id, amount FROM datago_qualities_report WHERE command_id=$command_id AND create_time=$now; -- 取已经检查完成的结果
                         |SET $rule_id, $threshold_type, $threshold_value := SELECT id, threshold_type, threshold_value FROM datago_qualities_rules WHERE command_id=$command_id;
                         |
                         |SET $status := 'normal';
                         |CASE $threshold_type
                         |	WHEN 'greater' THEN
                         |		IF $amount > $threshold_value THEN
                         |			SET $status := 'abnormal';
                         |		END IF;
                         |	WHEN 'equals' THEN
                         |		IF $amount == $threshold_value THEN
                         |			SET $status := 'abnormal';
                         |		END IF;
                         |	WHEN 'day' THEN
                         |		SET $previous_amount := SELECT amount FROM datago_qualities_report WHERE command_id=$command_id AND id<$report_id ORDER BY id DESC LIMIT 1;
                         |		IF Math.abs(($amount - $previous_amount) / $previous_amount) * 100 > $threshold_value THEN
                         |			SET $status := 'abnormal';
                         |		END IF;
                         |	WHEN 'week' THEN
                         |		SET $avg_amount := SELECT AVG(amount) FROM (SELECT amount FROM datago_qualities_report WHERE command_id=$command_id AND id<$report_id ORDER BY id DESC LIMIT 7) A;
                         |		IF Math.abs(($amount - $avg_amount) / $avg_amount) * 100 > $threshold_value THEN
                         |			SET $status := 'abnormal';
                         |		END IF;
                         |	WHEN 'two-week' THEN
                         |		SET $avg_amount := SELECT AVG(amount) FROM (SELECT amount FROM datago_qualities_report WHERE command_id=$command_id AND id<$report_id ORDER BY id DESC LIMIT 14) A;
                         |		IF Math.abs(($amount - $avg_amount) / $avg_amount) * 100 > $threshold_value THEN
                         |			SET $status := 'abnormal';
                         |		END IF;
                         |	WHEN 'month' THEN
                         |		SET $avg_amount := SELECT AVG(amount) FROM (SELECT amount FROM datago_qualities_report WHERE command_id=$command_id AND id<$report_id ORDER BY id DESC LIMIT 30) A;
                         |		IF Math.abs(($amount - $avg_amount) / $avg_amount) * 100 > $threshold_value THEN
                         |			SET $status := 'abnormal';
                         |		END IF;
                         |END CASE;
                         |
                         |UPDATE datago_qualities_reports SET rule_id=$rule_id, reprot_result=$status WHERE id=$report_id;
                         |
                         |IF $status == 'abnormal' THEN
                         |	SET $owners := SELECT owners FROM datago_qualities_rules WHERE command_id=$command_id;
                         |	SET $title := SELECT title FROM qross_jobs WHERE id=$job_id;
                         |	IF $owners != '' THEN
                         |		SET $recipients := SELECT CONCAT('<', `fullname`, '>', `email`) AS recipient FROM datago_users WHERE id IN ($owners!) -> FIRST COLUMN -> JOIN '; ';
                         |		SEND MAIL '数据质量检查预警 ———— ' + $title
                         |			CONTENT '''数据质量检查任务：$title <br/> 发现不满足数据质量的问题，请及时处理。'''
                         |			TO $recipients;
                         |	END IF;
                         |END IF;""".stripMargin

        PQL.recognizeParametersIn(sentence).forEach(println)

        val select = DataSource.QROSS.querySingleValue("SELECT info FROM td WHERE id=100").asText("")
        PQL.openFile("/sql/args.sql").place(select).run()

        System.exit(0)

        val row = new Json("""{"action_id":5089,"job_id":2,"task_id":3584,"command_id":26,"task_time":"20201012112122","record_time":"2020-10-12 11:21:22","start_mode":"manual_start","command_type":"shell","command_text":"java -jar hello.jar ${ $task_time FORMAT \"yyyy-MM-dd\" }","overtime":0,"retry_limit":0,"job_type":"scheduled","title":"Test Job 2","owner":"吴佳妮<wujini@outlook.com>"}""").parseRow("/")
        val text = "java -jar hello.jar ${ $task_time FORMAT \"yyyy-MM-dd\" }"
        text.$restore(new PQL("", DataHub.DEFAULT).set(row), "").print

        if (args.nonEmpty) {
            args(0) match {
                case "ps" =>
                    ps$ef("qross-test").foreach(println)
                case "destroy" =>
                    println(destroy("test.sh"))
                case "end" =>
                    println(end("test.sh"))
                case _ =>
                    println(kill(args(0).toInt))
            }
        }
        else {
            //println("LACK OF ARGS.")
        }
    }
}
