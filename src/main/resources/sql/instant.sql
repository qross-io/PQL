PRINT '''@KEEPER_HTTP_ADDRESS:@KEEPER_HTTP_PORT/task/instant/#{jobId}''';
PRINT 'more={"jobId":#{jobId},"dag":"#{dag}","params":"#{params}","commands":"#{commands}","delay":0}';
REQUEST JSON API '''@KEEPER_HTTP_ADDRESS:@KEEPER_HTTP_PORT/task/instant/#{jobId}?more={"jobId":#{jobId},"dag":"#{dag}","params":"#{params}","commands":"#{commands}","delay":0}''' METHOD 'PUT';
PARSE "/" AS ROW;

--
--REQUEST JSON API 'http://localhost:8080/api/test/post?id=1' METHOD 'POST';
--PARSE '/' AS ROW;
