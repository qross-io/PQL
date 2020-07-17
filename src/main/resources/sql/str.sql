DEBUG ON;

REQUEST JSON API '''http://@KEEPER_HTTP_ADDRESS:@KEEPER_HTTP_PORT/test/json?id=1&name=Tom'''
    METHOD 'PUT'
    SEND DATA { "id": 2, "name": "Ted" };
PARSE "/";
