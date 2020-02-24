DEBUG ON;

OPEN 'mysql.adb';
SET $WHERE1 := "AND 1=1 ";

IF '#{clientCompanyId}' IS DEFINED THEN
    SET $WHERE1 := $WHERE1 + " AND client_company_id =#{clientCompanyId} ";
END IF;

IF '#{callType}' IS DEFINED THEN
    SET $WHERE1 := $WHERE1 + " AND call_type =#{callType} ";
END IF;


SET $WHERE2 := "AND 1=1 ";

IF '#{clientCompanyId}' IS DEFINED THEN
    SET $WHERE2 := $WHERE2 + " AND client_company_id =#{clientCompanyId} ";
END IF;

IF '#{repayment_type}' IS DEFINED THEN
    SET $WHERE2 := $WHERE2 + " AND repayment_type =#{repayment_type} ";
END IF;

SELECT a.stat_month,
IFNULL(b.case_num,0) AS case_num,
IFNULL(b.case_sum,0) AS case_sum,
IFNULL(b.repay_num,0) AS repay_num,
IFNULL(b.repay_sum,0) AS repay_sum
 FROM
(
SELECT  DISTINCT CONCAT(LEFT(day,4),SUBSTR(day FROM 6 FOR 2)) AS stat_month
FROM zichan360bi_ods.calendar
WHERE day BETWEEN DATE_ADD(CONCAT(LEFT(CURRENT_DATE,7),'-01'),INTERVAL -5 MONTH) AND CURRENT_DATE) a
LEFT JOIN
(SELECT a.stat_month,IFNULL(a.case_num,0) AS case_num,IFNULL(a.case_sum,0 ) AS case_sum,
IFNULL(b.repay_num,0) AS repay_num,IFNULL(b.repay_sum,0) AS repay_sum
FROM
(
SELECT CONCAT(LEFT(stat_date,4),SUBSTR(stat_date FROM 6 FOR 2)) AS stat_month,
SUM(withdraw_num) AS case_num,SUM(withdraw_total_money) case_sum
FROM zichan360bi_dw.dw_automatic_task_case
WHERE stat_date BETWEEN DATE_ADD(CONCAT(LEFT(CURRENT_DATE,7),'-01'),INTERVAL -5 MONTH)
AND CURRENT_DATE
$WHERE1!
GROUP BY 1 ) a
LEFT JOIN
(SELECT CONCAT(LEFT(repay_time,4),SUBSTR(repay_time FROM 6 FOR 2)) AS stat_month,
count(id) AS repay_num,SUM(repayment_money) AS repay_sum
FROM zichan360bi_ods.ods_automatic_task_repayment
WHERE repay_time BETWEEN CONCAT(DATE_ADD(CONCAT(LEFT(CURRENT_DATE,7),'-01'),INTERVAL -5 MONTH),' 00:00:00')
AND CURRENT_TIMESTAMP
 $WHERE2!
GROUP BY 1 )  b
ON a.stat_month = b.stat_month) b
ON a.stat_month = b.stat_month;