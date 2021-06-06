DEBUG ON;

PRINT;
PRINT '源数据库类型: #{source_database_name}';
PRINT '源数据库连接名: #{source_connection_name}';
PRINT '目标数据库类型: #{destination_database_name}';
PRINT '目标数据库连接名: #{destination_connection_name}';
PRINT '------------------------------------------------------------';
PRINT '流转开始';
-- 执行逻辑为
-- 大数据量的迁移采用源端切分数据量(块)，目标端并行拉取数据块的机制
-- 首先，需要定义$pagesize ，此参数表示源端每次查询数据的最大数量，默认10000
-- 抽取数据时分块机制会事先计算主键的起点和终点(参数#{start_point}、#{end_point}决定)，进行数据切分，
-- 依次执行查询语句(参数#{block_sentence})，注意的是：如查询语句 SELECT name, score FROM table1 WHERE id>@-- -- {id} AND id<=@{id} 必须指定占位符@{id}，并且名称必须与数据表中自增主键名的一致
-- 接下来会在目标端进行插入前的语句执行，参数#{prep_sentence} 如 delete from ...
-- 最后会进行数据的插入，由参数#{batch_sentence}控制，如：inter into......
-- 需要注意的是参数#{block_sentence}与#{batch_sentence} 中的字段 顺序位置，数量，必须一致

IF $pagesize IS UNDEFINED THEN
	SET $pagesize := 10000;
END IF;

OPEN #{source_connection_name};
	SET $start := #{start_point};
	PRINT '''Start Point: $start''';
	SET $end := #{end_point};
	PRINT '''End Point: $end''';
	BLOCK FROM $start! TO $end! PER $pagesize! # #{block_sentence};
SAVE TO #{destination_connection_name};
	IF $prep_sentence IS NOT UNDEFINED THEN
		PREP # #{prep_sentence};
		PRINT '''删除或更新数据量: @AFFECTED_ROWS_OF_LAST_PREP''';
	END IF;
	BATCH # #{batch_sentence};
PRINT '''总计获取数据量: @TOTAL_COUNT_OF_RECENT_GET''';
PRINT '''总计流转数据量: @TOTAL_AFFECTED_ROWS_OF_RECENT_PUT''';
PRINT '流转结束';
PRINT '------------------------------------------------------------';
PRINT;
-- INVOKE io.qross.pql.GlobalFunction.put("TEST", "$a", "", 0);