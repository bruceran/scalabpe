# <a name="toc">目录</a>

[测试工具](#testtool)

[日志文件目录](#dir)

[logback.xml配置文件](#logback)

[框架使用到的错误码](#error)

# <a name="testtool">测试工具</a>

## 使用service runscala

	输入./service runscala  进入控制台，可直接输入代码测试类，用来测试辅助类特别方便, 如

	scala>import jvmdbbroker.core._
	scala>import jvmdbbroker.flow._
    scala>new java.util.ArrayList()
    scala>Thread.sleep(50)
    scala>jvmdbbroker.flow.Utils.checkInclude(s,"123") 测试自己写的辅助类object Utils里的checkInclude方法

## 使用service runtest

    输入./service runtest testcasefile 可自动将 testcasefile 文件中定义的test case发给scalabpe
    testcasefile 不传则默认为 testcase/default.txt文件

    runtest 工具默认将请求发给本机，端口取自 config.xml中的<SapPort>节点;
	如config.xml中配置了<TestServerAddr>host:port</TestServerAddr>, runtest工具会读此配置并发送到该地址

    testcasefile 文件格式定义:

        * 一个文件里可保存所有测试用例
        * 每行一个test case, 空行忽略
        * #开头的行为注释，忽略
        * serviceId,msgId,json串   json串对应该消息的参数
        * json串中，如果包含 x_repeat 属性，可用来指定该请求要重复发多少次，不指定则为1

## 对于使用了http server的服务

    可以直接通过命令行curl或浏览器来访问url查看调用结果和调用过程

[返回](#toc)

# <a name="dir">日志文件目录</a>

## 目录结构

	/opt/logs/xxx/   	xxx为项目名称，脚本会自动取当前目录名作为项目名称
				 auditlog/
				 	request_audit.log 	对外接口的请求响应日志文件
				 	csos_audit.log 		流程的内部处理流程日志文件
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

[返回](#toc)

# <a name="logback">logback.xml配置文件</a>

## 请求响应日志文件 request_audit.log 输出配置

	默认日志以info级别输出，如果要关闭，可以设置为warn
    <logger name="jvmdbbroker.RequestLog" level="info" additivity="false"><appender-ref ref="REQUESTLOG" /></logger>

	如果要关闭某个服务号的日志输出，如108服务号, 可如下配置
    <logger name="jvmdbbroker.RequestLog.108" level="warn" additivity="false"><appender-ref ref="REQUESTLOG" /></logger>

	如果要关闭某个服务号下某个特定消息的日志输出，如108服务号201消息号，可如下配置
    <logger name="jvmdbbroker.RequestLog.108.201" level="warn" additivity="false"><appender-ref ref="REQUESTLOG" /></logger>

## 流程内部调用日志文件 csos_audit.log 输出配置
	
	默认日志以info级别输出，如果要关闭，可以设置为warn
    <logger name="jvmdbbroker.CsosLog" level="info" additivity="false"><appender-ref ref="CSOSLOG" /></logger>

    如果要关闭某个服务号的日志输出，如231服务号, 可如下配置
    <logger name="jvmdbbroker.CsosLog.231" level="info" additivity="false"><appender-ref ref="CSOSLOG" /></logger>

    如果要关闭某个服务号下某个特定消息的日志输出，如231服务号12消息号，可如下配置
    <logger name="jvmdbbroker.CsosLog.231.12" level="info" additivity="false"><appender-ref ref="CSOSLOG" /></logger>

## HTTP SERVER请求响应日志文件 http/request_audit.log 输出配置

	默认日志以info级别输出，如果要关闭，可以设置为warn
    <logger name="jvmdbbroker.HttpRequestLog" level="info" additivity="false"><appender-ref ref="HTTPREQUESTLOG" /></logger>

	如果要关闭某个服务号的日志输出，如108服务号, 可如下配置
    <logger name="jvmdbbroker.HttpRequestLog.108" level="warn" additivity="false"><appender-ref ref="HTTPREQUESTLOG" /></logger>

	如果要关闭某个服务号下某个特定消息的日志输出，如108服务号201消息号，可如下配置
    <logger name="jvmdbbroker.HttpRequestLog.108.201" level="warn" additivity="false"><appender-ref ref="HTTPREQUESTLOG" /></logger>

## 插件的日志输出

	框架源码中很多插件的日志是以debug级别输出, 由于log/all.log默认是info级别，
    日志实际并不会输出，若要看到这些日志需调整为debug级别

    查看DB请求的SQL和实际参数，需设置<logger name="jvmdbbroker.plugin.DbClient" level="debug" ...

    查看MemCache实际的KEY,VALUE内容，需设置<logger name="jvmdbbroker.plugin.MemCacheClient" level="debug" ...

    查看REDIS实际的KEY,VALUE内容，需设置<logger name="jvmdbbroker.plugin.RedisClient" level="debug" ...

    查看AHT请求和响应原始内容，需设置<logger name="jvmdbbroker.plugin.http.HttpClientImpl" level="debug" ...

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
    * 对于数据库查询未找到匹配数据的情况，不会返回错误，而是返回0； 
      流程中应根据rowcount是否为0来判断是否有匹配数据

[返回](#toc)