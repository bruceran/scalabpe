# <a name="toc">目录</a>

[测试工具](#testtool)

[日志文件目录](#dir)

[日志文件格式](#format)

[日志数据上报和拦截](#report)

[logback.xml配置文件](#logback)

[框架使用到的错误码](#error)

# <a name="testtool">测试工具</a>

## 使用service runscala

	输入./service runscala 进入控制台，可直接输入代码测试类，用来测试辅助类特别方便, 如

	scala>import scalabpe.core._
	scala>import scalabpe.flow._
    scala>new java.util.ArrayList()
    scala>Thread.sleep(50)
    scala>scalabpe.flow.Utils.checkInclude(s,"123") 测试自己写的辅助类object Utils里的checkInclude方法

## 使用service runtest测试工具，简单版本, 简单版本目前不推荐再使用

    runtest工具会自动判断格式，若文件中包含 global: testcase:, 则认为是集成测试工具版本, 否则是简单版本

    以下是简单版本的使用说明：

    输入./service runtest testcasefile 可自动将 testcasefile 文件中定义的test case发给scalabpe
        testcasefile 不传则默认为 testcase/default.txt文件

    runtest 工具默认将请求发给本机，端口取自 config.xml中的<SapPort>节点;
	如config.xml中配置了<TestServerAddr>host:port</TestServerAddr>, runtest工具会读此配置并发送到该地址

    testcasefile 文件格式定义:

        * 一个文件里可保存多个测试用例
        * 每行一个test case, 空行忽略
        * #开头的行为注释，忽略
        * 每行格式为：serviceId,msgId,json串   json串对应该消息的参数
        * json串中，如果包含 x_repeat 属性，可用来指定该请求要重复发多少次，不指定则为1
        * 所有testcase串行执行

## 使用service runtest测试工具，集成测试版本

    runtest工具会自动判断格式，若文件中包含 global: testcase:, 则认为是集成测试工具版本, 否则是简单版本

    以下是集成测试版本的使用说明：

    输入./service runtest -a -d testcasefile1 testcasefile2 ... 
        -a 忽略enabled标志，运行所有测试用例
        --all_files        对testcase下所有txt文件运行测试
        -d 参数表示输出解析后的文本到标准输出，可不传
        -c --cover        生成覆盖率统计(只对函数定义，invoke调用，reply调用进行统计)
        testcasefile 不传则默认为 testcase/default.txt文件， 支持使用*通配符表示多个文件

    新的集成测试工具不需要单独先启动待测试的服务，测试工具本身会启动服务，完成测试，再关闭服务

    testcase运行的所有请求响应日志都在csos_audit.log中，request_audit.log中没有日志

    testcasefile 文件格式定义:

        * 所有缩进和行最后的空格忽略
        * #开头的行为注释，忽略
        * 每行最后的"空格#"开始的文本忽略, 注意，如果调用参数中有"空格#", 需在行最后加一个#以免被当成注释
        * 所有空行忽略
        * 所有非 global: testcase: define: mock: setup: teardown: assert: setup_http: teardown_http: assert_http: 开头的行自动合并到前一行, 
          合并是在去掉尾部的"空格#"再进行的, 方便对参数加注释

        * global 表示testcase的公共部分, 一个文件中只允许有一个global，global下define,mock,setup,teardown,setup_http,teardown_http
        * testcase 表示定义一个testcase, 一个文件中允许多个testcase, 每个testcase下有define,mock,setup,teardown,assert,setup_http,assert_http,teardown_http
        * define表示定义常量, 格式为：define: 参数列表,  参数列表的key可以加$前缀，也可不加，引用的时候必须加$
        * mock表示mock指令，定义一个接口的mock, 格式为：mock:服务名.消息名 req: 参数列表  res: 参数列表;
        * mock指令允许对同一个服务名消息名有多个mock, 这种情况req入参用于条件匹配，先匹配的优先，若无参数则匹配所有请求，若无匹配，则返回错误码-10242404
        * assert表示断言指令, 格式为：assert:服务名.消息名  id:xxx timeout:15000 req: 参数列表  res: 参数列表, res来的参数用于结果匹配, 
          只有完全匹配才算成功
        * assert_http表示http调用断言指令, 格式为：assert_http:url  其它同assert指令
        * setup表示定义testcase启动前要调用的接口，一般用于初始化数据, 
          格式为：setup:服务名.消息名 id:xxx timeout:15000 req: 参数列表  res: 参数列表, 要求返回码为0才能继续, setup可以类似assert一样加断言
        * setup_http表示http setup指令, 格式为：setup_http:url  其它同setup指令
        * teardown表示定义testcase完成后要调用的接口，一般用于清理数据, 
          格式为：teardown:服务名.消息名  id:xxx timeout:15000 req: 参数列表  res: 参数列表，忽略返回码
        * teardown_http表示http teardown指令, 格式为：teardown_http:url  其它同teardown指令
        * setup,assert,setup_http,assert_http指令里的timeout可以不写，默认为15000,表示15秒; 
        * setup,assert,setup_http,assert_http指令里的id可以不写，如果调用的请求和响应要被引用，则必须定义id, id可以带$前缀，也可以不带，引用的时候必须带$前缀; 
          teardown里定义id意义不大
        * 如果多个testcase有公共的指令需要每次重复执行，可以指定通过属性extends:basetestcase来指定要重复执行的公共指令，extends指令只支持一级
        * global节点可以配置remote:属性, 配置为0使用本地模式，否则使用远程模式，可配置为具体的ip:port来指定访问, 或配置为1使用本地config.xml里的配置
          远程模式的testcase文件里不允许使用mock会报错
        * global节点里针对http调用的特殊属性：
            http_base_url:指定http调用的base_url，实际url是此url+指令中的url
            http_method:指定http method, 可以是get,post,put,delete, 默认为post
            http_ok_field:响应中的哪个field被认为是错误码，默认为return_code 
            http_ok_value:响应中的什么值被认为是成功，默认为0
            http_rest:要调用的服务是否是rest风格的（支持get,post, put,delete,入参放在content里，json格式），默认为false
        * setup_http,assert_http指令支持http cookie,http 302重定向
        * setup_http,assert_http指令支持http header属性发送，文件上传和文件下载

        * 运行顺序：

            define,mock,setup,assert,teardown,setup_http,assert_http,teardown_http的运行顺序除了teardown/teardown_http以外都是按定义的顺序执行, teardown/teardown_http指令不管定义在哪里总在最后执行； 
            相同类别的指令按定义顺序执行;

            全局指令
            testcase级别： testcase级别指令 -> testcase级别teardown
            全局teardown
            
            所有global和testcase的define都是全局有效的，id必须唯一；若相同则后定义的会覆盖前面的值
            global的mock全局有效，testcase级的mock只在本testcase有效
            setup/assert/setup_http/assert_http里的id是全局有效的，若相同，则后定义的执行后会覆盖前面的值；
            可以跨testcase按id引用调用
            mock安装的是表达式，被调用的时候时候才会被执行，mock中的表达式在运行时执行，可以象assert指令里的表达式一样引用
            setup指令和assert指令的区别是：setup必须成功，若失败就结束测试；

        * 参数列表: 参数列表的格式为 left1=right1 left2=right2 以空格和等号作为分隔符, 连续的非空字符加一个等号表示一个key的开始，
          如果=要作为值而不是分隔符，则有可能需要用\=进行转义
        * 不等判断：在assert/setup/assert_http/setup_http指令的响应中和mock指令的请求中，=号的作用是比较而不是赋值， 这2种特殊情况可以使用!=表示不等于

        * 参数列表里的右值若为{},[]则会解析为结构体对象和数组对象再返回，若要返回原始json串，需加一个s:前缀
        * $code 是res参数列表中一个特殊key，表示返回码; 如果不定义$code则表示$code为0

        * 值引用：
            $xxx 来引用define定义的常量
            $xxx.req.yyy id为xxx的assert/setup/assert_http/setup_http的请求里的yyy值
            $xxx.res.zzz id为xxx的assert/setup/assert_http/setup_http的响应里的zzz值
            $this.req.yyy 当前请求的assert/setup/assert_http/setup_http或mock的请求里的yyy值
            $this.res.zzz 当前请求的assert/setup/assert_http/setup_http或mock的响应里的zzz值
            $xxx.req.yyy[0] id为xxx的assert/setup/assert_http/setup_http的请求里的yyy下标为0的值
            $xxx.res.zzz.m id为xxx的assert/setup/assert_http/setup_http的响应里的zzz结构体里的m的值
            $xxx.res.yyy[1].abc id为xxx的assert/setup/assert_http/setup_http的响应请求里的yyy下标为1的结构体里的abc的值

            注意：在assert响应里引用当前响应里的值可以直接用key来引用

        * 支持函数调用，函数调用需带()，分为全局函数和对象上的函数; 函数的参数也可以使用表达式;  
            $xxx() 来引用全局函数xxx
            $contact(abc,def) 来引用全局函数contact, 带2个参数abc, def
            $abc.length() 来引用abc对象的length函数
            $xxx.req.yyy.matches(abc.*) id为xxx的assert/setup/assert_http/setup_http的请求里的yyy值是否匹配正则表达式 abc.*
            $xxx.res.yyy[1].size() id为xxx的assert/setup/assert_http/setup_http的响应请求里的yyy下标为1的结构体的长度

        * 内置函数
             内置的全局函数：$now(), $uuid()
             内置的string对象函数： toString(), length(), size(), matches(), left(), right(), contains(), indexOf(), substring()
             内置的数值比较函数： gt(), ge(), lt(), le()
             内置的map对象函数： toString(),  size(), toJson(), contains()
             内置的array对象函数： toString(),  size(), toJson(), contains()

        * 字符串中可以嵌入 ${...} 或 $xxx, $xxx应只用在无二义的情况下; 
        * 支持表达式的概念，实际上所有值引用，函数调用，字符串中嵌入都是表达式，可以使用表达式的地方如下：
            所有右值可以使用表达式, 包括define,mock,setup,assert,teardown,setup_http,assert_http,teardown_http,mock
            assert/setup/assert_http/setup_http响应的左值可以使用表达式; 
            mock请求的左值可以使用表达式; 

        * 可以通过在FlowHelper类中定义函数进行功能扩展; 如果不想在FlowHelper中定义，可以在global指令中定义pluginObjectName:xxx属性来指定你自己的类名(必须带路径)
            自定义全局函数的所有参数必须是String类型
            自定义对象函数的第一个参数必须是对象的类型，目前只允许: String,HashMapStringAny,ArrayBufferAny; 其余参数是对象函数的参数，必须是String类型
            自定义函数的返回结果类型不限制，可以是String,Int,HashMapStringAny,ArrayBufferAny等
            示例：
                def add(a:String,b:String) = a.toInt+b.toInt
                def size(m:HashMapStringAny) = m.size

        * 支持assert:sleep 特殊消息, 有些服务端流程是异步，需要做下sleep才能保证后续请求成功, 请求参数列表为  s=秒 ms=毫秒 m=要输出消息
        * 支持assert:stop 特殊消息, 出现断言失败的时候，可以在对应断言后加个stop停止执行后续用例便于分析问题, 无请求参数

        * 转义：\= \. \, \( \) \[ \] \$ \\ \{ \}会转义为=.,()[]$\{} 大部分情况不需使用转义，只在表达式解析有问题的时候可加上转义让表达式正确解析

        * 示例：

            testcase:t1 
                define: $year=1998
                mock:service999.echo req: name=hello1998 res: msg=hello2006
                mock:service999.echo req: name=hello2006 res: msg=hello2016

                assert:service999.echo id:$s1 
                    req: name=hello${year}
                    res: msg=hello2006  msg.length()=9
                assert:service999.echo 
                    req: name=$s1.res.msg
                    res: msg=hello2016  msg.matches(.*2016)=true

            testcase:t2
                define: $year=1998
                mock:service999.echo req: name.contains(${year})=true res: msg=${this.req.name} $year
                mock:service999.echo req: res: msg="${this.req.name}"

                assert:service999.echo 
                    req: name=hello${year}
                    res: msg=hello1998 1998

            testcase:t3
                define: $year=1998
                mock:service999.echo req: name!=1996 res: msg=100
                mock:service999.echo req: res: msg=101

                assert:service999.echo req: name=${year} res: msg!=101
                assert:service999.echo req: name=1996 res: msg=101


## 对于使用了http server插件的服务

    可以直接通过命令行curl或浏览器来访问url

[返回](#toc)

# <a name="dir">日志文件目录</a>

## 目录结构

	/opt/logs/xxx/   	xxx为项目名称，脚本会自动取当前目录名作为项目名称
				 auditlog/
				 	request_audit.log 	对外接口的请求响应日志文件
				 	csos_audit.log 		流程的内部处理流程日志文件
                    request_stat.log  对外接口汇总日志
                    sos_stat.log   内部处理接口汇总日志
                    request_summary.log 对外请求/连接情况汇总日志
				 	http/
				 		request_audit.log 	http服务的请求响应日志文件
				 		access.log 	http服务的访问日志文件,类似nginx日志
				 log/
				 	console.log 启动信息，启动停止脚本会检查此文件的内容
				 	all.log   	框架或业务流程输出的日志
				 	root.log  	引用的其它第三方jar包输出的日志
				 	soc.log 	avenue客户端连接日志	

## 说明

    * linux下脚本默认直接在/opt/logs下建立日志目录
    * windows下/opt/目录会自动在项目所在盘符的根目录下创建
    * 在流程中可以直接使用println(..)输出信息到all.log来分析问题

[返回](#toc)

# <a name="format">日志文件格式</a>

## auditlog/request_audit.log

    该文件分割符为: 一个逗号2个空格

    示例：
        2015-11-05 00:00:03.144,
        10.129.20.129,
        43939,
        10.129.20.129,
        43939,
        -10086,
        -10087,
        10.129.20.126:38700,
        020128144665280011510000,
        1,
        55611,
        2,
        10.129.20.128,
        2015-11-05 00:00:00.115,
        3,
        ,
        ,
        ,
        ,
        ,
        appId:991001023^_^username:991001023-0-769431828^_^socId:180.112.238.183:40462^_^platform:1,
        failReason:,
        0

    格式：
        TIMESTAMP  输出日志的时间戳
        PROXY_IP 代理IP
        PROXY_PORT 代理端口
        CLIENT_IP 客户端IP
        CLIENT_PORT 客户端端口
        APPID  扩展包头的APPID
        AREAID  扩展包头的AREAID
        SOCID  客户端标识
        REQUESTID 请求ID
        XHEAD_UNIQUE_ID 扩展包头里的uniqueId, 若没有值则记录1
        SERVICEID  服务号
        MSGID  消息号
        SERVER_IP 服务端IP
        RECEIVE_TIME 收到请求的的时间
        ELAPSE_TIME, 耗时，等于 TIMESTAMP - RECEIVE_TIME
        DUMMY 无意义
        DUMMY 无意义 1.2.6版本开始为：服务名.消息名
        IDX1  索引字段1
        IDX2  索引字段2
        IDX3  索引字段3
        REQUEST_BODY 请求参数，参数之间分割符为 ^_^
        RESPONSE_BODY 响应参数，参数之间分割符为 ^_^
        RESPONSE_CODE 错误码

## auditlog/csos_audit.log 

    该文件分割符为: 一个逗号2个空格

    示例：
        2016-04-22 00:00:00.002,
        020021146125439999210000:4,
        302,
        101,
        312,
        1,
        10.129.20.22:12052,
        ,
        ,
        miscservice.phoneuserinfocontrol,
        user_client_ip:127.0.0.1^_^src_code:10^_^type:1^_^phone:^_^xid:1003260494^_^mid:1003260494^_^nickname:^_^image_id:^_^u_sex:^_^u_career:^_^u_phone:^_^u_qq:^_^u_email:^_^u_city:^_^u_birthday:^_^mail_address:^_^b_uid:^_^other_flag:,
        fail_reason:^_^mid:2165378584^_^phone:18689939034^_^nickname:mike^_^image_id:http://gmm.sdo.com/gmm/img/8.jpg^_^u_sex:-1^_^u_career:^_^u_phone:18689939034^_^u_qq:350127202^_^u_email:^_^u_city:^_^u_birthday:1971-01-01^_^create_time:2015-12-28 21:33:09^_^homepage_header_url:^_^credit_info:{"s_sharing_hours":24,"s_fee_discount":100,"b_eval_total":0,"s_judge_hours":72,"s_credit_value":5,"b_credit_lvl":0,"s_eval_total":0,"b_eval_good":0,"s_credit_lvl":0,"s_eval_good":0,"b_wait_verify_hours":24,"b_credit_value":5}^_^b_uid:1003258903^_^is_complete:0^_^mail_address:,
        4,
        0

    格式：
        TIMESTAMP 输出日志的时间戳
        REQUESTID 请求ID
        OUTSERVICEID 外部服务号
        OUTMSGID  外部消息号
        INSERVICEID 内部服务号
        INMSGID 内部消息号
        SOS_ADDR 被调用服务的地址
        DUMMY 无意义
        DUMMY 无意义 1.2.6版本开始为：扩展包头里的uniqueId
        SERVICE_NAME 服务名.消息名
        REQUEST_BODY 请求参数，参数之间分割符为 ^_^
        RESPONSE_BODY 响应参数，参数之间分割符为 ^_^
        ELAPSE_TIME 耗时
        RESPONSE_CODE 错误码

    csos_stat.log和request_audit.log文件通过REQUESTID关联
    根据request_audit.log里的REQUESTID查找csos_stat.log就能查到该请求的所有处理过程

## auditlog/http/request_audit.log 

    该文件分割符为: 一个逗号2个空格

    示例：
        2016-06-24 09:04:04 475,   
        10.152.21.16:63292,   
        /ticketpay/listcard,   
        266,   
        9,   
        -1,   
        -1,   
        ,   
        ,   
        ,   
        channelAmount=0&rmbAmount=0&DeviceId=358239056710181&LogId=e78238ef-2c83-42b3-a6d6-713935daec5b&jsessionId=f4afd085ea2a456c90c8cdac68d81191,   
        10.152.21.55:8861,   
        13,   
        0,   
        {"data":{"listIsNotNull":0,"find":0},"return_code":0,"return_message":"success"},   
        tmstamp:0,   
        sigstat:0,   
        3e86956d61084893bb69728c74ef4797,   
        fh.sdo.com

    格式：
        TIMESTAMP  输出日志的时间戳
        CLIENT_IP 客户端IP:PORT
        HTTP_URI 路径
        SERVICEID  服务号
        MSGID  消息号
        DUMMY 无意义  
        DUMMY 无意义        
        DUMMY 无意义
        DUMMY 无意义        
        DUMMY 无意义        
        REQUEST_BODY 请求参数
        SERVER_IPPORT 服务端IP:PORT
        ELAPSE_TIME  耗时
        RESPONSE_CODE 错误码
        CONTENT 响应内容
        DUMMY 无意义        
        DUMMY 无意义   
        REQUESTID 请求ID
        HTTP_HOST 域名                

## auditlog/http/access.log 

    该文件分割符为: tab字符

    示例：
        2016-06-24 09:03:00.018 53      
        10.152.21.16:63164      
        10.152.21.16:63164      
        GET     
        fh.sdo.com      
        /fh/pay/entrance        
        token=T6DE8D398FFCF4C6690A8A051BB0FD117&sign= ...       
        302     
        0  
        Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build ...

    格式：
        TIMESTAMP  输出日志的时间戳
        CLIENT_IP 客户端IP:PORT
        PROXY_IP 代理IP:PORT
        HTTP_METHOD 方法
        HTTP_HOST  域名
        HTTP_URI  路径
        PARAMETERS 参数
        HTTP CODE  HTTP状态码
        CONTENT_LENGTH  输出长度
        USER_AGENT 用户代理

## auditlog/request_stat.log

    示例：2014-11-24 00:00:00,  301,  55,  127.0.0.1:10070,  1,  0,  0,  1,  0,  0,  0,  0
    格式：时间（1分钟输出一次，无数据不输出），服务号，消息号，客户端连接标识，成功数，失败数，10ms数，50ms数， 250ms数， 1秒数，3秒数，其它数

## auditlog/sos_stat.log

    示例：2014-11-24 00:02:00,  350,  2,  12,  0,  0,  12,  0,  0,  0,  0,  0
    格式：时间（1分钟输出一次，无数据不输出），服务号，消息号，成功数，失败数，10ms数，50ms数， 150ms数，250ms数， 1秒数，其它数, 超时数(-10242504数量)

## auditlog/request_summary.log

    示例：2014-11-24 00:00:00,  request[1/1/0],  client[0/0/17]
    格式：时间（1分钟输出一次，无数据不输出），request[总数/成功数/失败数] 客户端连接[新建连接数/断开连接数/当前连接数]

[返回](#toc)

# <a name="report">日志数据上报和拦截</a>
    
## 格式

    当config.xml里配置了

        <ReportUrl>http://api.monitor.sdo.com/stat_more_actions.php</ReportUrl>

        框架会以如下的方式上报 request_summary.log 里的数据

        POST /stat_more_actions.php HTTP/1.0
        Content-Type: application/x-www-form-urlencoded
        action[]=1734|1739|1740|1743,0  间隔内连接数
        &action[]=1734|1739|1740|1744,0 间隔内断开连接数
        &action[]=1734|1739|1740|1745,568 当前连接数
        &action[]=1734|1739|1740|1746,6520 间隔内请求数
        &action[]=1734|1739|1740|1747,4339 间隔内成功应答数
        &action[]=1734|1739|1740|1748,2181 间隔内失败应答数
        &ip=10.129.58.21

        action[]的格式为： 固定配置项,数量

    当config.xml里配置了

        <DetailReportUrl>http://api.monitor.sdo.com/stat_pages.php</DetailReportUrl>

        框架会以如下的方式上报request_stat.log里的数据

        POST /stat_pages.php HTTP/1.0
        Content-Type: application/x-www-form-urlencoded
        action[]=1734|1764|1780,16_206,2  对外业务请求数
        &action[]=1734|1765|1783,16_206,2  对外业务响应成功数
        &action[]=1734|1766|1786,16_206,0  对外业务响应失败数
        &action[]=1734|1767|1789,16_206,0  对外业务10ms内响应数
        &action[]=1734|1768|1792,16_206,2  对外业务50ms内响应数
        &action[]=1734|1769|1795,16_206,0  对外业务250ms内响应数
        &action[]=1734|1770|1798,16_206,0  对外业务1s内响应数
        &action[]=1734|1771|1774,16_206,0  对外业务3s内响应数
        &action[]=1734|1772|1777,16_206,0  对外业务other时间段响应数
        &ip=10.129.58.21

        action[]的格式为： 固定配置项,服务号_消息号,数量

        框架会以如下的方式上报sos_stat.log里的数据

        POST /stat_pages.php HTTP/1.0
        Content-Type: application/x-www-form-urlencoded
        action[]=1734|1749|1801,7,106  子服务请求数
        &action[]=1734|1750|1804,7,2  子服务响应成功数
        &action[]=1734|1751|1807,7,104  子服务响应失败数
        &action[]=1734|1752|1810,7,106 子服务10ms内响应数
        &action[]=1734|1753|1813,7,0 子服务50ms内响应数
        &action[]=1734|1754|1816,7,0  子服务150ms内响应数
        &action[]=1734|1755|1819,7,0  250ms内响应数
        &action[]=1734|1756|1759,7,0  1s内响应数
        &action[]=1734|1757|1762,7,0   子服务other时间段响应数
        &action[]=1734|2893|2895,7,0  子服务响应超时数
        &ip=10.129.58.21

        action[]的格式为： 固定配置项,服务号,数量

## 拦截

    用户如希望实现自己的监控服务器，可实现自己的http服务器，只要按上述格式处理
    上报的数据即可。

[返回](#toc)

# <a name="logback">logback.xml配置文件</a>

## 请求响应日志文件 request_audit.log 输出配置

	默认日志以info级别输出，如果要关闭，可以设置为warn
    <logger name="scalabpe.RequestLog" level="info" additivity="false"><appender-ref ref="REQUESTLOG" /></logger>

	如果要关闭某个服务号的日志输出，如108服务号, 可如下配置
    <logger name="scalabpe.RequestLog.108" level="warn" additivity="false"><appender-ref ref="REQUESTLOG" /></logger>

	如果要关闭某个服务号下某个特定消息的日志输出，如108服务号201消息号，可如下配置
    <logger name="scalabpe.RequestLog.108.201" level="warn" additivity="false"><appender-ref ref="REQUESTLOG" /></logger>

## 流程内部调用日志文件 csos_audit.log 输出配置
	
	默认日志以info级别输出，如果要关闭，可以设置为warn
    <logger name="scalabpe.CsosLog" level="info" additivity="false"><appender-ref ref="CSOSLOG" /></logger>

    如果要关闭某个服务号的日志输出，如231服务号, 可如下配置
    <logger name="scalabpe.CsosLog.231" level="info" additivity="false"><appender-ref ref="CSOSLOG" /></logger>

    如果要关闭某个服务号下某个特定消息的日志输出，如231服务号12消息号，可如下配置
    <logger name="scalabpe.CsosLog.231.12" level="info" additivity="false"><appender-ref ref="CSOSLOG" /></logger>

## HTTP SERVER请求响应日志文件 http/request_audit.log 输出配置

	默认日志以info级别输出，如果要关闭，可以设置为warn
    <logger name="scalabpe.HttpRequestLog" level="info" additivity="false"><appender-ref ref="HTTPREQUESTLOG" /></logger>

	如果要关闭某个服务号的日志输出，如108服务号, 可如下配置
    <logger name="scalabpe.HttpRequestLog.108" level="warn" additivity="false"><appender-ref ref="HTTPREQUESTLOG" /></logger>

	如果要关闭某个服务号下某个特定消息的日志输出，如108服务号201消息号，可如下配置
    <logger name="scalabpe.HttpRequestLog.108.201" level="warn" additivity="false"><appender-ref ref="HTTPREQUESTLOG" /></logger>

## 插件的日志输出

	框架源码中很多插件的日志是以debug级别输出, 由于log/all.log默认是info级别，
    日志实际并不会输出，若要看到这些日志需调整为debug级别

    查看DB请求的SQL和实际参数，需设置<logger name="scalabpe.plugin.DbClient" level="debug" ...

    查看MemCache实际的KEY,VALUE内容，需设置<logger name="scalabpe.plugin.MemCacheClient" level="debug" ...

    查看REDIS实际的KEY,VALUE内容，需设置<logger name="scalabpe.plugin.RedisClient" level="debug" ...

    查看AHT请求和响应原始内容，需设置<logger name="scalabpe.plugin.http.HttpClientImpl" level="debug" ...

## logVar
    
    request_audit.log日志文件中只有入参出参，如果希望将流程变量或中间变量也加入该日志，
    可使用logVar(key,value)方法


[返回](#toc)

# <a name="error">框架使用到的错误码</a>

## 错误码分类

    编解码错误 -10242400
    网络错误  -10242404
    服务或消息未找到 -10242405
    队列满或队列阻塞引起的超时 -10242488
    内部错误 -10242500
    超时  -10242504

    缓存中数据未找到 -10245404

## 说明

    * 所有的错误码都在 src/core.scala/ResultCodes 类里进行定义
    * 框架里将所有出现的错误码都归类到上述错误码之一
    * 对于数据库查询未找到匹配数据的情况，不会返回错误 -10245404，而是返回0； 
      流程中应根据rowcount是否为0来判断是否有匹配数据

[返回](#toc)
