



/*
 * 接口标题
 * #测试链接
 * # jdbcDriver 驱动类
 * # connectionString 链接信息
 * # userName 用户名（可空）
 * # passWord 密码（可空）
 * @return 连接成功返回空字符串，否则返回出错信息
 */

## test-connection | GET | username=&password= |
IF $username != '' THEN
    RUN SHELL java -Dfile.encoding=@CHARSET! -jar @RUNNING_DIR!qross-worker-@QROSS_VERSION!.jar -driver $jdbc_driver -url $connection_string -username $userName -password $passWord;
ELSE
    RUN SHELL java -Dfile.encoding=@CHARSET! -jar @RUNNING_DIR!qross-worker-@QROSS_VERSION!.jar -driver $jdbc_driver -url $connection_string;
END IF;


/*
 * 接口标题
 * 新增数据库链接
参数：{
"databaseName" : "mysql " ,
"connectionName" : "cehsi" ,
"description" : "测试" ,
"jdbcDriver" : "com.mysql.jdbc.Driver" ,
"connectionString" : "jdbc:mysql://localhost:3306/datax_web" ,
"userName" : "root" ,
"passWord" : "123456"
}
 * @return
 */
## add-connection | POST |@userid

set $a1 :=  """  null  , """ ;
if $databaseName is   null or $databaseName  == ''  then  set $a1 :=  """   null ,    """  ;
else set $a1 :=  """  '$databaseName'  ,  """  ;
end if ;

set $b1 :=  """  null  , """ ;
if $connectionName is   null or $connectionName  == ''  then  set $b1 :=  """   null ,    """  ;
else set $b1 :=  """  '$connectionName'  ,  """  ;
end if ;

set $c1 :=  """  null  , """ ;
if $description is   null or $description  == ''  then  set $c1 :=  """   null ,    """  ;
else set $c1 :=  """  '$description'  ,  """  ;
end if ;

set $d1 :=  """  null  , """ ;
if $jdbc_Driver is   null or $jdbc_Driver  == ''  then  set $d1 :=  """   null ,    """  ;
else set $d1 :=  """  '$jdbc_Driver'  ,  """  ;
end if ;


set $e1 :=  """  null  , """ ;
if $connection_String is   null or $connection_String  == ''  then  set $e1 :=  """   null ,    """  ;
else set $e1 :=  """  '$connection_String'  ,  """  ;
end if ;

set $f1 :=  """  null  , """ ;
if $userName is   null or $userName  == ''  then  set $f1 :=  """   null ,    """  ;
else set $f1 :=  """  '$userName'  ,  """  ;
end if ;

set $g1 :=  """  null  , """ ;
if $passWord is   null or $passWord  == ''  then  set $g1 :=  """   null ,    """  ;
else set $g1 :=  """  '$passWord'  ,  """  ;
end if ;



insert into qross_connections (database_name,connection_name,description,jdbc_driver,
                               connection_string,username,password,creator,mender,create_time )
values ( $a1!  $b1!  $c1! $d1!   $e1!  $f1!  $g1!    @userid , null , now() ) ;


/*
 * 接口标题
 * 删除链接
 * #id  int
 * @return
 */
## delete-connection | DELETE | id=0 |
delete  from  qross_connections  where id = #{id} ;


/*
 * 接口标题
 * 更新链接
 * 参数：{
"id":1,
"databaseName" : "mysql" ,
"connectionName" : "cehsi_001" ,
"description" : "测试_hh" ,
"jdbcDriver" : "com.mysql.jdbc.Driver" ,
"connectionString" : "jdbc:mysql://localhost:3306/datax_web" ,
"userName" : "root" ,
"passWord" : "0000"
}
 * @return
 */
## update-connection | PUT |

set $a1 :=  '' ;
if $databaseName is  not null and $databaseName  != ''  then  set $a1 :=  """   database_name = '#{databaseName}' ,    """  ;
else set $a1 :=  '' ;
end if ;

set $b1 :=  '';
if $connectionName is  not null and $connectionName  != ''  then  set $b1 :=  """   connection_name = '#{connectionName}'  ,    """  ;
else set $b1 :=  '' ;
end if ;

set $c1 :=  '' ;
if $description is  not null and $description  != ''  then  set $c1 :=  """  description = '#{description}',   """  ;
else set $c1 :=  '' ;
end if ;

set $d1 :=  '' ;
if $jdbc_Driver is  not  null and $jdbc_Driver  != ''  then  set $d1 :=  """    jdbc_driver = '#{jdbc_Driver}',   """  ;
else set $d1 :=  '' ;
end if ;

set $e1 :=  '' ;
if $connection_String is  not null and  $connection_String  != ''  then  set $e1 :=  """   connection_string= '#{connection_String}' ,    """  ;
else set $e1 :=  ''  ;
end if ;

set $f1 :=  '';
if $userName is  not  null and $userName  != ''  then  set $f1 :=  """   username = '#{userName}' ,   """  ;
else set $f1 :=  ''   ;
end if ;

set $g1 :=  '' ;
if $passWord is  not null or $passWord  != ''  then  set $g1 :=  """    password = '#{passWord}'  ,     """  ;
else set $g1 :=  ''  ;
end if ;

update qross_connections
  set  $a1!  $b1!  $c1!  $d1!  $e1!  $f1!  $g1!
       mender = @userid , update_time = now()
 where id = #{id} ;

/*
 * 接口标题
 * 根据id查询链接信息
 * #id  int
 * @return row
 */
## query-connection | GET | id=0 |
select  id ,  database_name as databaseName , connection_name as connectionName , description , jdbc_driver as jdbc_Driver ,
        connection_string  as connection_String , username as userName , password as passWord , creator , mender ,
        create_time as createTime
from qross_connections  where id = #{id} ;

/*
 * 接口标题
 * 查询链接集合
 * #connectionName 链接名称
 * # databaseName 数据源类型
 * # creator  创建者
 * page 页码 | pagesize 每页条数
 * @return ARRAY
 */
## query-all | GET | page=0&pagesize=9&connectionName=&databaseName=&creator= |
set $where := '' ;
if $connectionName is not  null and $connectionName != ''  then
SET $where := """  t.connection_name like CONCAT(CONCAT('%', '#{connectionName}'), '%')  """  ;
elsif  $databaseName is not  null and $databaseName != ''  then
SET $where := """  t.database_name like CONCAT(CONCAT('%', '#{databaseName}'), '%')  """  ;
elsif  $creator is not  null and $creator != ''  then
SET $where := """  u.username like CONCAT(CONCAT('%', '#{creator}'), '%')   """  ;
else set $where := "1=1" ;
end if ;

 VAR $list := SELECT t.id as id ,  database_name as databaseName , connection_name as connectionName,description,jdbc_driver as jdbc_Driver,
        connection_string as connection_String , u.username as  creator , t.create_time  as createTime
FROM qross_connections AS t
left join  datago_users as u  on t.creator = u.id
where  $where!
limit ${ #{page} * #{pagesize}  }, #{pagesize} ;

set $count:= SELECT count(1)
            FROM qross_connections AS t
                     left join  datago_users as u  on t.creator = u.id
            where $where! ;

OUTPUT {
    "list":$list ,
    "count": $count
};

/*
 * 接口标题
 * 可根据条件查询链接数量
 * # connectionName  链接名称
 * # databaseName 数据源类型
 * # creator  创建者
 * @return  int
 */
##  tatol-count | GET | connectionName=&databaseName=&creator= |
set $where := '';
if $connectionName is not  null and $connectionName != ''  then
SET $where := """  t.connection_name like CONCAT(CONCAT('%', '#{connectionName}'), '%')  """  ;
elsif  $databaseName is not  null and $databaseName != ''  then
SET $where := """  t.database_name like CONCAT(CONCAT('%', '#{databaseName}'), '%')  """  ;
elsif  $creator is not  null and $creator != ''  then
SET $where := """  u.username like CONCAT(CONCAT('%', '#{creator}'), '%')   """  ;
else set $where := "1=1" ;
end if ;

SELECT count(1)
FROM qross_connections AS t
left join  datago_users as u  on t.creator = u.id
where $where! ;


