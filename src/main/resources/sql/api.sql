DEBUG ON;

OPEN 'mysql.adb';
SET $WHERE := "WHERE 1=1 ";

IF '#{clientCompanyId}' IS DEFINED THEN
    SET $WHERE:= $WHERE + " AND client_company_id =#{clientCompanyId} ";
END IF;

IF '#{callType}' IS DEFINED THEN
    SET $WHERE:= $WHERE + " AND call_type =#{callType} ";
END IF;

IF '#{startDate}' IS DEFINED THEN
    SET $WHERE:= $WHERE + " AND stat_date >= '#{startDate}' ";
END IF;

IF '#{endDate}' IS DEFINED THEN
    SET $WHERE:= $WHERE + " AND stat_date <= '#{endDate}' ";
END IF;

IF '#{realationShip}' IS DEFINED THEN
    SET $WHERE:= $WHERE + " AND relationship = #{realationShip} ";
END IF;


SET $num := select COUNT(1) from zichan360bi_dw.dw_automatic_task_case $WHERE!;


IF $num != 0 THEN
    select
    ROUND(sum(case when withdraw_total_money between 0 and 500 then withdraw_num else 0 end) / sum(case when withdraw_total_money between 0 and 9000 then withdraw_num else 0 end),2) as '0-500',
    ROUND(sum(case when withdraw_total_money between 501 and 1000 then withdraw_num else 0 end) / sum(case when withdraw_total_money between 0 and 9000 then withdraw_num else 0 end),2) as '501-1000',
    ROUND(sum(case when withdraw_total_money between 1001 and 2000 then withdraw_num else 0 end) / sum(case when withdraw_total_money between 0 and 9000 then withdraw_num else 0 end),2) as '1001-2000',
    ROUND(sum(case when withdraw_total_money between 2001 and 3000 then withdraw_num else 0 end) / sum(case when withdraw_total_money between 0 and 9000 then withdraw_num else 0 end),2) as '2001-3000',
    ROUND(sum(case when withdraw_total_money between 3001 and 5000 then withdraw_num else 0 end) / sum(case when withdraw_total_money between 0 and 9000 then withdraw_num else 0 end),2) as '3001-5000',
    ROUND(sum(case when withdraw_total_money between 5001 and 7000 then withdraw_num else 0 end) / sum(case when withdraw_total_money between 0 and 9000 then withdraw_num else 0 end),2) as '5001-7000',
    ROUND(sum(case when withdraw_total_money between 7001 and 9000 then withdraw_num else 0 end) / sum(case when withdraw_total_money between 0 and 9000 then withdraw_num else 0 end),2) as '7001-9000',
    ROUND(sum(case when withdraw_total_money > 9000 then withdraw_num else 0 end) / sum(case when withdraw_total_money between 0 and 9000 then withdraw_num else 0 end),2) as '9000'
    from zichan360bi_dw.dw_automatic_task_case
    $WHERE!;
ELSE
   SELECT id from zichan360bi_dw.dw_automatic_task_case $WHERE!;
END IF;
