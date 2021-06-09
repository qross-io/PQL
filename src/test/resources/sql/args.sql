DEBUG ON;

FILE VOYAGE '''c:/io.Qross/Home/pql/datax.json'''
				WITH @ARGUMENTS
				TO '''c:/io.Qross/Home/temp/test.json''';


EXIT CODE 0;
-- 4d0d4ba6866e671f46d0d449a57a94f0cf7831273358af778b7f51607061736d
-- c6aaa20d244da5dfd00da55b329a1af6379be431e63486214bc66a103bcf1480
REQUEST JSON API 'https://oapi.dingtalk.com/robot/send?access_token=4d0d4ba6866e671f46d0d449a57a94f0cf7831273358af778b7f51607061736d'
--REQUEST JSON API 'https://oapi.dingtalk.com/robot/send?access_token=c6aaa20d244da5dfd00da55b329a1af6379be431e63486214bc66a103bcf1480'
    METHOD 'POST'
    DATA {
        "at": {
            "atMobiles": [ 18618171102 ]
        },
        "text": {
            "content": "报警"
        },
        "msgtype": "text"
    };
PARSE "/";

-- 告警数据库:mysql_reader\n告警表:test\n负责人:tom\n告警类型:强规则\n告警信息:异常数n据大与设定阈值!

/*
curl --location --request POST 'https://oapi.dingtalk.com/robot/send?access_token=c6aaa20d244da5dfd00da55b329a1af6379be431e63486214bc66a103bcf1480' \
--header 'Content-Type: application/json' \
--data-raw '{
    "at": {
        "atMobiles":[
            "18618171102"
        ],
        "atUserIds":[
            ""
        ],
        "isAtAll": false
    },
    "text": {
        "content":"告警数据库:mysql_reader\n告警表:test\n负责人:tom\n告警类型:强规则\n告警信息:异常数n据大与设定阈值!报警了"
    },
    "msgtype":"text"
}'

*/
