
* 约定
* 对外提供服务的TCP端口
* 服务端SOS配置
* 管理HTTP端口
* 异步流程引擎使用的线程数
* 异步日志
* 流程里可使用的KEY/VALUE配置参数设置
9) 对外开放或关闭服务
11) 同步线程池配置
12) 错误码/错误信息支持配置


* 对外提供服务的HTTP端口
13) 定时任务配置
14) 远程SOS服务配置
15) MemCache服务配置
16) 本地缓存服务（进程内缓存)配置
17) Redis服务配置
18) 本地持久化队列配置
19) 消息队列配置
20) 消息队列接受者配置
21) DB配置
22) 同步DB（支持事务）配置
23) AHT配置

# 约定

  xml中所有节点名都是首字母大写，属性名都是首字母小写

# 对外提供服务的TCP端口

  `<SapPort>9898</SapPort>`

  如设置为0，则表示不需要对外启动sos，只作为一个job服务

# 管理HTTP端口

  `<CohPort>9899</CohPort>`

  如设置为0，则表示不需要管理功能

  目前支持：

  http://host:port/SelfCheck.do 自检, 符合监控部的格式要求
  http://host:port/NotifyChanged.do 刷新进程内缓存
  http://host:port/Dump.do 写进程内资源（线程数，连接数等）信息到all.log日志中用于分析


# 异步流程引擎使用的线程数

  <ThreadNum>4</ThreadNum>

  对异步流程引擎，4个线程足够

# 同步线程池配置

  所有异步流程都共用<ThreadNum>4</ThreadNum>配置项，所以不允许在线程内发生阻塞
  异步线程池只有一个, 但是可以额外配置多个线程池用于可能会发生阻塞的消息

  <SyncedFlowCfg  threadNum="n">
    <ServiceId>999.3,999.4</ServiceId>
  </SyncedFlowCfg>

  threadNum: 指定该线程池的线程数, 若未配置则默认等于<ThreadNum>4</ThreadNum>里的值

  此配置为消息级别，用逗号隔开多个消息，可指定对应的消息使用一个独立的线程池;
  如需将该服务的所有消息都加入此独立线程池，可使用serviceid.*表示

# 异步日志

  <ReportUrl>http://api.monitor.sdo.com/stat_more_actions.php</ReportUrl>

    ReportUrl用于报告服务总体情况：连接数，请求数，不分服务消息，这个数据上报作用不大, 如未配置，则不报
    ReportUrl上报对应的日志数据在request_summary.log文件中

  <DetailReportUrl>http://api.monitor.sdo.com/stat_pages.php</DetailReportUrl>
  <DetailReportServiceId>320,321,...</DetailReportServiceId>

    DetailReportUrl用于报告对外服务和子服务的请求数（成功，失败），耗时角度的统计数据, 如未配置，则不报
    DetailReportUrl上报对应的日志数据在request_stat.log文件(对外服务)和sos_stat.log文件(子服务)中

    DetailReportServiceId用来控制需要上报哪些服务号的统计数字到监控系统，此参数不影响打日志；如未配置，则全部都上报

  <AsyncLogThreadNum>1</AsyncLogThreadNum >

    AsyncLogThreadNum为异步日志使用的线程数，默认为1, 一般不用设置

    上报数据给监控系统时如果出现网络错误会重试，每次间隔5秒钟，最多重试2次。如果超时，不重试。

  <AsyncLogWithFieldName>true</AsyncLogWithFieldName>

    在打印输入参数和输出参数的日志时，是否在值的前面输出参数名称；输出参数名称可方便查看数据； 默认为true

  <AsyncLogArray>1</AsyncLogArray>

    用于控制对数组类型日志打印前几条数据，默认为1, 只打印数组的第一条

  <AsyncLogPasswordFields>password,pass_word</AsyncLogPasswordFields>

    配置日志中要隐藏实际值的字段，配置了该字段，在request log, csos log中将会以 *** 代替实际值, 多个配置值用逗号分隔
    也可以使用此参数来隐藏lob类型字段的日志，否则日志可能会很长

  <AsyncLogDispatch defaultTarget="999.28">
        <Item serviceId="999" msgId="27" target="999.28"/>
  </AsyncLogDispatch>

    其中msgId可用*代替匹配所有消息; target可不配置，则取defaultTarget默认值
    可转发给本地服务或远程服务；为避免消息丢失，可在服务描述文件将消息设为必达消息, isAck="true"
    目标消息的服务描述文件要求：
      serviceId 服务号
      msgId 消息号
      kvarray string array, 请求，响应，流程变量

# 流程里可使用的KEY/VALUE配置参数设置

  <Parameter name="xxx">yyyy</Parameter>

  流程中可用Flow.router.getConfig("xxx",defaultValue) 获取到上述xxx对应的值yyy

# 对外开放或关闭服务

  <Parameter name="serviceIdsNotAllowed">999,977</Parameter>

  默认流程服务(包括子流程服务)都对外开放, 可用serviceIdsNotAllowed调整

  <Parameter name="serviceIdsAllowed">45601,45602</Parameter>

  默认非流程服务都不对外开放, 可用serviceIdsAllowed调整

# runtest 目标地址

  <TestServerAddr>host:port</TestServerAddr>

  此配置仅用于runtest测试工具，用来将请求发给远程服务而不是本地服务

# SOS配置

  <ServerSos
       host="*"
       threadNum="1"
       maxPackageSize="2000000"
       idleTimeoutMillis="180000"
       isSps="0"
       spsReportTo="55605:1"
       spsDisconnectNotifyTo="55605:111"
       isEncrypted="0"
       shakeHandsServiceIdMsgId="1:5"
       reverseServiceIds="0"
       timeout="30000"
       timeoutInterval="100"
       pushToIpPort="0"
       pushToIp="0"
       pushToAny="0" 
   >
        <ReverseIp>127.0.0.1</ReverseIp>
        <ReverseIp>127.0.0.2</ReverseIp>
        <ReverseIp>...</ReverseIp>
  </ServerSos>

  目前支持：

  host 绑定哪个网卡，默认绑定所有网卡
  threadNum 线程数，默认为2
  maxPackageSize 最大包长，默认为 2000000
  idleTimeoutMillis 超时断开连接时间，默认为180000, 3分钟

  isSps 是否启动sps模式 默认为0
      sps模式的特殊处理：
            1) 接收到的包会自动加入扩展包头spsId和socId, spsId为一个guid, socId为客户端连接和端口
            2) 每次和后端服务建立连接后会自动发送一个注册spsId的消息，使用spsReportTo配置参数
            3) 连接断开后会通知后端服务，使用spsDisconnectNotifyTo配置参数
            4) 反向调用时必须指定目标地址，可在流程中用invokeWithToAddr或者使用扩展包头的socId参数
  spsReportTo sps向route服务发送注册消息, 默认为 55605:1
  spsDisconnectNotifyTo sps向route服务发送连接断开消息, 默认为 55605:111
  isEncrypted 是否启用加密, 默认为0
  shakeHandsServiceIdMsgId 握手服务号消息号，默认为 1:5, isEncrypted 开启时只有此消息是明文，其他都是密文

  reverseServiceIds 定义反向调用的服务号, 默认为0, 多个用逗号分隔
  timeout 反向调用(从SOS主动发请求给SOC)的网络超时时间，默认为30000, 30秒
  timerInterval 反向调用请求的定时器间隔时间，默认为100毫秒
  pushToIpPort 推送给指定ip和端口的客户端, 默认为false, isSps开启时此开关自动设置为true
  pushToIp 推送给指定ip的客户端，按顺序轮询推送, 默认为false, isSps开启时此开关自动设置为false
  pushToAny 推送给任意客户端，按顺序轮询推送, 默认为false, isSps开启时此开关自动设置为false

  ReverseIp 配置允许反向调用到哪些IP上, 不设置，则可能调用到任意客户端

  流程中如何反向调用

   *)  如客户端是有状态的，使用invokeWithToAddr()调用客户端的接口, 其中toAddr：

              IP： 发给对应IP连接过来的客户端 可根据req.remoteIp获取
              IP:PORT 发给对应IP和端口连接过来的客户端 可根据req.remoteAddr获取
              IP:PORT:ID 发给指定连接 可根据req.connId获取

   *)  如客户端是无状态的，也可使用invoke() 进行反向调用, 此时toAddr为空，则请求可发给客户端的任意连接上
   *)  如果想限制接收反向调用的IP，可通过<ReverseIp>...来进行配置

# 对外提供服务的HTTP端口

    HTTP接入完全兼容HPS已有功能，使用插件的形式实现，配置如下：

    目录结构
          projecthome/ 项目根目录
                webapp/ web目录
                    static/ 静态文件目录
                    template/ 模板文件目录
                    upload/ 上传文件目录

    <HttpServerCfg port="9090"
        host="*"
        threadNum="2"
        timeout="30000"
        idleTimeout="45000"
        defaultVerify="false"
        osapDb="false"
        osapDbServiceId="62100"
        returnMessageFieldNames="xxx,xxx"
        sessionFieldName="jsessionId"
        sessionCookieName="JSESSIONID"
        sessionMode="1"
        jsonRpcUrl="/jsonrpc"

        cacheEnabled="true"
        cacheFileSize="25000"
        cacheFiles="html htm css js"
        httpCacheSeconds="86400"
        httpTemplateCache="true"
        httpTemplateCheckInterval="60"
        skipMinFile="false"
        enableMock="false"
        devMode="false"
        logUserAgent="false"
		maxContentLength="5000000"
        >

        <UrlMapping>
            <Item   charset="UTF-8"
                    requestContentType="application/x-www-form-urlencoded"
                    responseContentType="application/json"
                    caseInsensitive="false"
                    verify="false"
                    logFilters="regex1,regex2,regex3"
                    whiteIps="ipgrp1,ipgrp2"
                    bodyOnly="0"
                    encodeRequest="1"
                    plugin="">youyun/sendPhoneCheckCode,999,26</Item>
            ...
        </UrlMapping>

        <MimeTypes>
            <Item>contenttype ext1 ext2 ...</Item>
        </MimeTypes>

    </HttpServerCfg>

    如未配置HttpServerCfg节点, 则不启动http服务
    port 对外的端口, 此配置值必须手工指定
    host 绑定到哪个IP上, 默认是*, 不限
    threadNum 内部的线程数，因为只是转发，不需要很多线程，默认是2
    timeout 发给后端服务消息的超时时间，若超时直接给客户端超时响应
    idleTimeout 对keep-alive连接，超过此值由服务端关闭
    defaultVerify 接口默认是否需要签名授权, 默认为false
    osapDb 是否需要连接授权库，若此值为true,则还需配置 osapDbServiceId和一个对应的DbSosList, 默认为false
    osapDbServiceId osap查配置对应的服务描述文件的服务号，默认为 62100
        查配置对应的服务描述文件为预定义好，直接放在avenue_conf目录下使用，不用修改
        <DbSosList threadNum="1" desc="osap db">
            <ServiceId>62100</ServiceId>
            <MasterDb conns="1">
                <DefaultConn>service=jdbc:oracle:thin:@//10.240.17.38:1521/ebs10g user=osap2 password=osap2</DefaultConn>
            </MasterDb>
        </DbSosList>
        注意：在配置了要连接osap授权库的时候，如是配置未取到，则http监听端口不会打开，要取到配置后才会打开；
        每10分钟一次配置数据会自动刷新，也可调用 /NotifyChanged.do来立即刷新配置
    returnMessageFieldNames 从body中提取哪些field作为输出json串顶层的return message, 默认值为return_message,resultMessage,result_message,failReason,fail_reason
    sessionFieldName 服务描述文件中哪一项对应的是HTTP会话ID，默认是jsessionId
    sessionCookieName HTTP会话ID对应的cookie名，默认是JSESSIONID
    sessionMode sessionId模式 1=自动 2=手工，默认是自动
        自动模式 request中若无sessionId,会自动创建，会自动输出新的sessionId到cookie中,即使服务描述文件输出中未定义
        手工模式 request中若无sessionId,不会自动创建，由业务流程自己创建，要输出sessionId到cookie中,必须在服务描述文件输出中进行定义
    sessionIpBind sessionId是否和客户端ip绑定
    logUserAgent 是否在日志中输出UserAgent, 默认为0
	maxContentLength http请求post最大允许长度
	
    jsonRpcUrl 客户端一次发多个url给服务端对应的接收地址, 默认是 /jsonrpc

        参考 http://blog.csdn.net/mhmyqn/article/details/39718097，具体协议略有不同
        请求格式可以为：
            1) get方法  /jsonrpc?data=json
            2) post方法 /jsonrpc  post的内容为json
        json体为一个json数组，数组每一项为一个对象: jsonrpc为版本号，固定为2.0，id从1开始编号, method为url, params为参数，参数可以是url_encoded形式或json对象形式
        json请求示例：
            [
                {"jsonrpc":"2.0","id":"1","method":"/path/url1","params":"a=1&b=2"},
                {"jsonrpc":"2.0","id":"2","method":"/path/url2","params":{"a":"1","b":"2"}}
            ]
        响应格式为json数组
        json体为一个json数组，数组每一项为一个对象: jsonrpc为版本号，固定为2.0，id从1开始编号, result为一个json对象，就是hps的标准返回内容
        json请求示例：
            [
                {"jsonrpc":"2.0","id":"1","result":{"return_code":0,"return_message":"success","data":{...}}},
                {"jsonrpc":"2.0","id":"2","result":{"return_code":-1,"return_message":"error","data":{...}}},
            ]

    UrlMapping为url映射表，每一项Item代表一个配置, 每个配置有若干配置项，每个配置项都有默认值，一般不需要配置，具体如下：

        url 可以为固定路径，也可以配置为动态路径，目前动态路径最多支持2个连续动态参数，如 path1/.../pathN/:a/:b, 在解析的时候会从路径中取参数a和b

        caseInsensitive 获取参数是否区分大小写，默认为false, 区分大小写; 注意：url比较总是不区分大小写的
        charset 输入输出的字符集
        requestContentType 输入内容格式，默认为application/x-www-form-urlencoded, 若非这种形式则必须定义plugin来解析
		如果RequestContentType配置为 multipart/form-data, 则表明该请求为上传文件请求，对上传的文件，接收参数是一个名为files的结构体数组,
			结构体为：
				filename 文件名
				name 表单field名
				ext 文件后缀
				size 文件长度
				contentType 文件mimetype
				file 临时文件名, 保存在webapp/upload目录下以.tmp为后缀
        responseContentType 输出内容格式，默认为application/json, 若非这种形式则必须定义plugin来处理输出
        verify 是否要签名检查，若未配置则取defaultVerify
            如果要验签名：有配置verify plugin, 则优先使用插件校验，否则使用hps标准校验方法进行校验：md5校验, ip校验，权限校验
        whiteIps 白名单IP, 可配置多个ip组，如"ipgrp1,ipgrp2", 每个对应一个Parameter配置项
            <Parameter name="ipgrp1">127.0.0.3,127.0.0.2</Parameter>
            <Parameter name="ipgrp2">127.0.0.3,127.0.0.2</Parameter>
            白名单IP验证机制和verify验证机制独立，互不影响
        logFilters 输出日志要隐藏哪些内容，可配多个正则表达式，如"regex1,regex2,regex3", 每个对应一个Parameter配置项
            <Parameter name="regex1"><![CDATA[&?a=[^&]*]]></Parameter>
            <Parameter name="regex2"><![CDATA[&?b=[^&]*]]></Parameter>
            <Parameter name="regex3"><![CDATA[\"items\":\"[^\"]*\",?]]></Parameter>
        bodyOnly 在输出json部分，只输出body部分的json，不输出hps的return_code, return_message
        encodeRequest 对接收到的入参不按照服务描述文件进行编码过滤，用来接收一些未知的参数，比如转发接口
        plugin="plain|redirect|template|..."
        plugin中允许的值为预定义的值或者类名, 预定义的值有：
            plain 输出纯文本，从plainText参数中取值，若要调整参数名，可以用plugin="plain:参数名"形式
            redirect 输出一段html进行跳转，从redirectUrl参数中取值，若要调整参数名，可以用plugin="redirect:参数名"形式
            template 根据模板输出内容，必须用plugin="template:模板名"形式来指定模板;
                模板必须放在 template目录下，不带后缀名
            类名, 支持以下几种插件
                trait HttpServerPlugin 标记接口，所有插件必须有此标记
                trait HttpServerRequestParsePlugin 参数解析插件
                trait HttpServerRequestPostParsePlugin 解析后处理插件, 可对解析完毕的参数进一步做些调整
                trait HttpServerVerifyPlugin 签名处理插件
                trait HttpServerPreOutputPlugin 输出前参数调整插件，如只想对输出的json串的某些内容做调整
                trait HttpServerOutputPlugin  输出插件，plain,redirect,template 都是这种插件
            一个插件可以实现上述多个功能
        如果要进行服务端302重定向，不需使用插件，只要响应包中包含redirectUrl302并且非空，则做服务端302重定向

     日志文件：
        HttpServer的日志文件为request_audit.log，格式同hps的request_detail.log，仅输出动态请求的日志
        access日志为访问日志, 输出所有http日志
        使用jsonrpc一次发多个调用的时候，日志为每个请求单独输出，从requestId可区分，jsonrpc的requestId都带rpc前缀并带id序号
        logback中可配置http日志是否输出，可控制到消息级别
            <logger name="jvmdbbroker.HttpRequestLog" level="info" additivity="false"><appender-ref ref="HTTPREQUESTLOG" /></logger>
            <logger name="jvmdbbroker.HttpRequestLog.xxx" level="info" additivity="false"><appender-ref ref="HTTPREQUESTLOG" /></logger>
            <logger name="jvmdbbroker.HttpRequestLog.xxx.xxx" level="info" additivity="false"><appender-ref ref="HTTPREQUESTLOG" /></logger>
            <logger name="jvmdbbroker.HttpRequestLog.access" level="info" additivity="false"><appender-ref ref="HTTPACCESSLOG" /></logger>

     json转换:
       avenue协议只支持int,String，实际输出json的时候有时候需要将int转成string, 或将string转成number
       对深度嵌套的json, avenue协议不能支持，但avenue协议可以将后台拼好的json串以string形式返回，在输出时转换成json就可支持深度嵌套
       通过对服务描述文件的type项和struct里的field项支持classex属性来进行这种额外的转换
       classex可以配置几个值：
            string 将int转成string输出
            long 将string转成long输出
            double 将string转成double输出
            json 将string转成json串再输出, 这种方法可形成一个多级嵌套的json

     cookie支持：
          接口入参中 <field name="a" type="a_type" cookieName="xxx"/> 表示a的值从cookie xxx中获取
          接口出参中 <field name="a" type="a_type" cookieName="xxx" cookieOption="Path=/;Domain=.sdo.com;..."/> 表示把a作为cookie输出，cookieOption配置cookie选项，可选
          sessionId作为一种特殊的cookie, 无需上述这样配置，可直接通过sessionFieldName,sessionCookieName简化配置

     http header支持：
          接口入参中 <field name="a" type="a_type" headerName="xxx"/> 表示a的值从http header xxx中获取
          接口出参中 <field name="a" type="a_type" headerName="xxx"/> 表示把a输出到http header中

    cacheEnabled 对静态文件，是否启用cache功能，默认为启用
    cacheFileSize 对静态文件，低于此size的文件启用cache功能, 默认为25000字节
    cacheFiles 对静态文件，哪些后缀会启用cache功能，用空格分隔多个后缀，默认为 html htm css js
    httpCacheSeconds 对静态文件，输出的http头(expire,cache-control)允许客户端多长的缓存时间,默认为86400秒,若为0则不允许客户端缓存
    httpTemplateCache 对模板文件，是否启用缓存功能, 默认为true
    httpTemplateCheckInterval 对模板文件，多长时间检查一下时间戳是否发生变化，默认为60秒
    skipMinFile 对静态文件，如果访问的是.min.js .min.css 等带.min.文件，是否改为访问不带.min.的文件，默认为false
    enableMock 是否支持url mock, 若开启，则自动给所有url增加/mock再进行匹配，默认为false
    urlArgs 访问静态文件时的参数，默认配置为?, 仅用于输出到模板中
    devMode 开发模式下的总开关，若设置为true,则会自动调整其它开关：cacheEnabled和httpTemplateCache设置为false, httpCacheSeconds设置为0，skipMinFile设置为true，enableMock设置为true

    MimeTypes为mime-type映射表，每一项Item代表一种类型，默认已配置html,txt,css,js,xml,json,gif,jpeg,png,ico,tiff的支持
    如果有未支持的mime-type,可通过MimeTypes项来配置，格式示例： image/jpeg jpeg jpg jpe, 第一项为mime-type, 后面的为对应的各种后缀，可能有多个

    静态文件支持
          如果存在webapp/static目录，当url不存在时，返回http 404错误，而不是hps json错误
          支持If-Modified-Since, 若文件未变更，返回http 304错误
          不支持断点下载
          不支持上传文件
    模板文件支持
          模板文件的文件名要求带后缀，比如 a.xml.vm, 系统会根据后缀来自动生成content-type
          模板文件若分目录，则定义模板参数的时候需带相对路径
          支持两种格式:simple和velocity, 区分：如果模板文件的后缀是.vm,则是velocity格式，否则是simple格式
          simple格式语法：只支持 ${xxx} 访问响应体内的单个值 和 ${xxx.yyy}, 访问响应体内的map里的值 , 支持avnue协议的string,int,struct
          velocity格式语法：参考velocity语法手册 也支持上述的  ${xxx} ${xxx.yyy} 和其它高级功能，如条件，循环等  , 支持avnue协议的sting,int,struct,string array,int array, struct array
          ${domainName} ${contextPath} ${urlArgs} ${return_code} ${return_message} 为特殊值，可在模板文件中使用
            domainName 域名
            contextPath url的根一级目录
            urlArgs 静态文件的url参数，默认为?, 通过配置为不同值，如?v1, ?v2可强制客户端所有js,css,html失效重新从服务器下载最新版本
    jsonp支持
          如果入参中有一个jsonp=xxx, 且返回为json格式，则会转换为text/javascript, 格式为 xxx(jsondata);


12) 错误码，错误信息支持配置

    		<ErrorCodeCfg localCacheServiceId="xxx">
    			<Service serviceId="999" resultCodeField="resultCode" resultMsgField="resultMsg"/>
    			<Service serviceId="998" resultCodeField="resultCode" />
    			...
    		</ErrorCodeCfg>

        在流程中不用关心body里的resultCode, resultMsg, 通过配置来处理；
        body里的resultCode值应该总是和avenue包头的code值相同；

        resultCodeField 配置对应 body的 key;
        resultMsgField 配置对应 body的 key, resultMsgField可以不配置，如果未配置，则localCacheServiceId也不需指定;
        对于配置了resultCodeField的响应中，会自动将resultCode设置为avenue code值；
        对于配置了resultMsgField的响应中，则查询localCacheServiceId获取错误信息并设置到body中； 如果flow中指定了则不覆盖
        code=0时不写resultMsgField

        localCacheServiceId应该是一个local cache服务，不可以是其它异步服务;
        要求该local cache服务的get方法的：key为resultCode,value为resultMsg

13) 定时任务配置

    		<QuartzCfg>
    			<Cron serviceId="999" msgId="2">0/2 * * * * ?</Cron>
    			<Repeat serviceId="999" msgId="1" startDelay="1" repeatInterval="1"/>
    		</QuartzCfg>

    		按服务号消息号定义定时任务：
    		方式1) 用Cron表达式，定时调用该服务号该消息号
    		方式2) 用Repeat表达式，指定初始间隔秒数和后续每次间隔秒数
    		调用接口时无入参，也不等待返回值；建议：实现该消息的流程一开始就reply(0)

14) 远程SOS服务配置

    		配置远程SOS服务的服务号和多个地址

    		<SosList >
    			<ServiceId>888,889</ServiceId>
    			<ServerAddr>127.0.0.1:9888</ServerAddr>
    			<ServerAddr>127.0.0.2:9888</ServerAddr>
    			...
    		</SosList>

    		SosList节点上的可配置属性, 以下属性一般都不需要设置，默认值就够了：

    		threadNum="2"  为消息投递线程数，默认为2
    		timeout="30000" 为每个消息的超时时间，默认为30000, 表示30秒
    		retryTimes="2" 消息发出后连接中断的消息会重发给其它服务器，默认为2次; 如果未找到一个可用连接，直接返回网络错误，不重试，不等待
    		connectTimeout="15000" 连接超时，默认为15000, 表示15秒
    		pingInterval="60" 心跳间隔，默认为60, 表示60秒
    		maxPackageSize="40000" 最大包长, 默认为 40000 字节
    		connSizePerAddr="8" 每个服务器建立的长连接数量，默认为 8 个, 消息发送采用轮询方式
    		timerInterval="100" 内部超时定时器的间隔时间，默认为100，表示100毫秒
    		reconnectInterval="1" 连接断开后的重连间隔时间，默认为1，表示1秒

15) MemCache服务配置

    <CacheThreadNum>2</CacheThreadNum>
    <CacheWriteThreadNum>1</CacheWriteThreadNum>

    CacheThreadNum 为默认的读线程数 get  getanddelete getandcas 使用
    CacheWriteThreadNum 为默认的写线程数 set delete 使用

    每一个CacheSosList节点都会启动独立的2个线程池，一个读，一个写；而不是共用线程池

		<CacheSosList readThreadNum="2" writeThreadNum="5">
		    <ServiceId>990,45612</ServiceId>
				<ServerAddr>10.241.37.37:11211</ServerAddr>
				<ServerAddr>10.241.37.37:11211</ServerAddr>
				...
		</CacheSosList>

    readThreadNum="2" writeThreadNum="5" 若未配置，则用默认值; 默认节点未配置则为1

    3种模式：

      ArrayHash模式: 数据hash后取余定位服务器，数据只有1份；
      ConHash模式: 数据用一致性hash算法，数据只有1份；
      Master/Slave模式：数据写2份；

      如果只配置了1个ServerAddr，且未指定ConHash,ArrayHash, 为ArrayHash方式
      如果配置了2个ServerAddr，且未指定ConHash,ArrayHash, 为Master/Slave模式；
      如果超过2个ServerAddr，且未指定ConHash,ArrayHash, 为ArrayHash方式
      如果指定了 <ConHash/> 或 <ArrayHash/> 则为指定模式

    服务描述文件区别:

      特殊的code:

        exptime_type固定为10000，其 default 属性为该缓存的缓存超时时间，单位为秒
        code < 10000 为 key，可以有多个
        code > 10000 为 value, 可以有多个

      <message name="get" id="1">  id必须为1, name没有限制
      <message name="set" id="2">  id必须为2, name没有限制
      <message name="delete" id="3">  id必须为3, name没有限制
      <message name="getanddelete" id="4"> id必须为4, name没有限制
      <message name="incrby" id="6"> id必须为6, name没有限制
      <message name="decrby" id="7"> id必须为7, name没有限制

    写入缓存中时:

      key为BPE服务号_key1_key2_key3_...
      value为value1^_^value2^_^value3^_^...

      key,value的顺序和服务描述文件里的requestParameter,responseParameter里field的顺序无关，只和code大小相关

    在编写服务描述文件的  requestParameter,responseParameter 时，field 顺序不要紧， 但建议和code顺序保持一致。

16) 本地缓存服务（进程内缓存)配置

 		<ConfigDb>
			<ServiceId>40998,...</ServiceId>
			<ConnString>service=xxx user=xxx password=xxx</ConnString>
		</ConfigDb>

    将数据库内的数据加载到内存中，并缓存在本地文件系统中;
    如数据库连不通，本地文件可用，也可启动；如都不可以用，则启动失败；
    ConnString中的service=可省略, 兼容以前的配置

    缓存文件格式为 list_xxx,  xxx为服务号， 文件格式为，一行表示一个配置项, 字段之间用\t分隔

    jvmdbbroker里支持只使用本地文件，这时配置可简化为

 		<ConfigDb>
			<ServiceId>100</ServiceId>
		</ConfigDb>

		这种情况要求本地的 list_服务号 文件必须存在

    任何invoke接口访问本地缓存服务，都不会发生线程切换，invoke()形式上是通过callback继续执行，syncedInvoke()则是用返回值返回

    服务描述文件区别:

    兼容MemCache服务

    a) 支持的方法

        <message name="get" id="1"> (dbbroker里还需配置一个set方法，jvmdbbroker里去除了)
        <message name="getarray" id="50"> 可根据一个key返回一组配置
        <message name="getall" id="51"> 返回所有配置

    b) 增加了一个特殊节点，用来配置查询语句，如果只使用本地文件，可不配置

      <sql>SELECT APPID,APPCODE FROM TBAPPCODEAMOUNTWHITELIST</sql>

      sql select结果的顺序必须和 code 顺序一一对应, 不看名字，只看顺序;
      sql select结果的顺序和get方法的入参顺序无关;

17) Redis服务配置

    redis服务提供缓存数据的持久化功能

		<RedisReadThreadNum>2</RedisReadThreadNum>
		<RedisWriteThreadNum>1</RedisWriteThreadNum>

    RedisReadThreadNum 为默认的读线程数 get  getanddelete getandcas 使用
    RedisWriteThreadNum 为默认的写线程数 set delete 使用

    每一个RedisSosList节点都会启动独立的2个线程池，一个读，一个写；而不是共用线程池

    RedisSosList节点上的 readThreadNum="2" writeThreadNum="5" 若未配置，则用默认值; 默认节点未配置则为1

    和memcache一样有3种模式

    MasterSlave时，可使用下面的配置

  		<RedisSosList readThreadNum="2" writeThreadNum="5">
  		    <ServiceId>990,45612</ServiceId>
  				<MasterServerAddr>10.241.37.35:6379</MasterServerAddr>
  				<SlaveServerAddr>10.241.37.35:6379</SlaveServerAddr>
  		</RedisSosList>

    或者用memcache一样的配置，

      <RedisSosList readThreadNum="2" writeThreadNum="5">
  		    <ServiceId>990,45612</ServiceId>
  				<ServerAddr>10.241.37.37:11211</ServerAddr>
  				<ServerAddr>10.241.37.37:11211</ServerAddr>
  				...
  		</RedisSosList>

      加<ArrayHash/> <ConHash/> 或者不加都支持，和memcache一样

      服务描述文件，KEY,VALUE格式和memcache一样

18) 本地持久化队列配置

    <LocalQueueCfg threadNum="2" receiverServiceId="879" maxSendTimes="5" retryInterval="5000">
        <ServiceId>878,...</ServiceId>
        <Msg msgId="1" maxSendTimes="5" retryInterval="5000"/>
        ...
    </LocalQueueCfg>

    <ConnLocalQueueCfg concurrentNum="20" receiverServiceId="879" maxSendTimes="5" retryInterval="30000">
        <ServiceId>878,...</ServiceId>
        <Msg msgId="1" maxSendTimes="5" retryInterval="5000"/>
        ...
    </ConnLocalQueueCfg>

    数据写入本地持久化队列后然后立即从receiverServiceId(该服务需自己配置流程)得到通知

    receiverServiceId 接收服务号,必须定义
    maxSendTimes 最多发送次数，默认为60次
    retryInterval 每次发送间隔,默认为5000
    threadNum 接收回调的线程数，默认为1，一般为1就足够了

    Msg节点，可按消息配置不同的重发次数和重试间隔时间，若无配置，则取默认值

    ConnLocalQueueCfg为增强版本地持久化队列，允许队列中的数据以concurrentNum="20"进行并行处理，其它配置兼容LocalQueueCfg
    concurrentNum 并行执行数量，默认为1，在推送，通知的时候可设置此值来提高并行度

    本地队列服务描述文件差异:

    特殊的code:

      <type name="queueName_type" class="string" code="10000" />

    队列名称: 要求配置一个code为10000的值，每个消息中都必须带这个参数来指定队列名称
    通常本地队列的接收者需要将消息分发给不同的远程接收者，比如发商户通知;

    本地队列在回调receiverServiceId时，只有得到0的返回值才会发下一条。
    不同的队列可以并行调用，但是同一个队列的消息是按顺序一条条调用的。

    为避免不同的远程接收者的数据彼此影响，调用者应通过queueName来区分不同的接收者, 提高并发程度，避免彼此影响。

    本地队列开销很小，不用担心队列数量会对性能有影响。

    本地队列对应的持久化文件默认在data/localqueue目录下

    我们实际经常需要给商户发通知，每个商户有自己的通知地址，通常可以用appId作为queueName。如果商户的notifyUrl可在入参中传递，则队列名中应加上notifyUrl的hashCode

    对receiverServiceId服务描述文件的要求:

        1) 本地队列的msgId和receiverServiceId服务描述文件的msgId是一一对应的。
        2) 不需要定义返回参数，只检查返回码
        3) 入参 (request field的name属性) 没有queueName
        4) 入参 (request field的name属性) 如果是以下特殊值，可额外获取一些发送信息

            x_sendCount  含义：当前发送次数
            x_isLastSend 含义: 为1表示是最后一次发送，为0表示不是最后一次发送
            x_sendTimeUsed 含义：从开始发送到当前时间，不包括最后一次调用的时间, 最后一次发送耗时可用System.currentTimeMillis-req.receivedTime得到
            x_maxSendTimes 含义：配置的maxSendTimes值，用于某些特殊情况下流程里做控制

            可以用来记录日志，写数据库等操作

19) 消息队列配置

    <MqCfg plugin="xxx">
        <ServiceId>991,992</ServiceId>
			  <Connection>service=failover:(tcp://10.132.17.201:61616)?timeout=1000 username=sdouser password=des:a1f1869a55d5fa63</Connection>
			  <Destination serviceId="991" queueName="com.sdo.billing.test1" persistent="true"/>
			  <Destination serviceId="992" queueName="com.sdo.billing.test2" persistent="true"/>
    </MqCfg>

    将消息发送给远程ActiveMQ服务
    基于服务号发送，每个服务号对应一个队列
    队列中的消息内容为一个json串，包括请求参数中的所有值，额外加上以下几个字段：

      messageId 服务器请求ID
      messageSourceIp 服务器IP
      messageTimestamp 时间
      messageType 消息号

    收到消息会立即写入本地queue文件，然后从本地queue文件取出发送给远程MQ, 所以不管远程网络是否可用总是立即返回成功;
    本地queue文件中的消息不会丢失，重启后也继续存在，会一直重试直到发送成功
    唯一可能失败就是出现本地 IO失败, 比如磁盘慢，有坏道等
    一般使用invokeWithReply发送消息到此服务就可以

    本地队列对应的持久化文件默认在data/mq目录下

    服务描述文件中所有消息都不需要response字段，只检查错误码，其它无特殊之处。

    内部实现时每个队列对应一个发送线程，避免队列之间彼此影响。

    如果队列的序列化是非标准格式，可以使用plugin属性指定插件类名，插件类需实现MqSelialize接口, 自定义插件类不再插入默认的messageId,messageSourceIp,messageTimestamp,messageType4个值, 由插件类自行处理

20) 消息队列接受者配置

    <MqReceiverCfg receiverServiceId="879" maxSendTimes="5" retryInterval="5000" plugin="xxx">
			  <Connection>service=failover:(tcp://10.132.17.201:61616)?timeout=1000 username=sdouser password=des:a1f1869a55d5fa63</Connection>
			  <Connection>service=failover:(tcp://10.132.17.201:61616)?timeout=1000 username=sdouser password=des:a1f1869a55d5fa63</Connection>
			  <Destination queueName="com.sdo.billing.test1" />
			  <Destination queueName="com.sdo.billing.test2" />
			  ...
    </MqReceiverCfg>

    消息接收者插件和MQ队列插件配套使用， 接收到MQ服务器的数据，先写到本地队列中，再使用和本地队列一样的方式调用receiverServiceId

    接受者一般对每个MQ服务器单独配置Connection,而不是放在一个Connection中，否则有些服务器上的消息队列会接收不到。
    Destination可配置多个，会认为每个Connection上都有对应的Destination存在；

    本地队列对应的持久化文件默认在data/mqreceiver目录下

    本地队列名的划分是根据json消息的 messageType 消息号，每个消息号对应一个独立队列
    receiverServiceId服务描述文件和MQ队列插件的服务描述文件一一对应，不过可多获取以下特殊值, 不关心可不配置：

        messageId 服务器请求ID, MQ队列插件发送时加入的值
        messageSourceIp 消息来源服务器IP, MQ队列插件发送时加入的值
        messageTimestamp 时间, MQ队列插件发送时加入的值
        messageType 消息号, MQ队列插件发送时加入的值

        x_sendCount  含义同本地队列
        x_isLastSend 含义同本地队列
        x_sendTimeUsed 含义同本地队列

    如果队列的序列化是非标准格式，可以使用plugin属性指定插件类名，插件类需实现MqDeselialize接口, 自定义插件类不再支持默认的messageId,messageSourceIp,messageTimestamp,messageType4个值, 由插件类自行处理

21) DB配置

    <LongTimeSql>500</LongTimeSql> 耗时SQL配置参数，默认为500毫秒，超过此阀值的sql将被以WARN级别输出到日志文件中,便于定位db性能问题

    服务描述文件差异:

    类型映射关系:

    	<message name="QueryTimes" id="1">
    		<sql><![CDATA[
                    SELECT outtimes,intimes,begintime,endtime FROM TBSNDAIDBILLINGTIMESSET
                    WHERE sndaid=:1 and appid=:2 and areaid=:3 and rownum < 2
            ]]></sql>
    		<requestParameter>
    			<field name="sndaId" type="sndaId_type" to=":1" default="xxx" columnType="xxx"/>
    			<field name="appId" type="appId_type" to=":2" />
    			<field name="areaId" type="areaId_type" to=":3" />
    		</requestParameter>
    		<responseParameter>
    			<field name="rowcount" type="rowcount_type" from="$ROWCOUNT"/>
    			<field name="outtimes" type="outtimes_type" from="$result[0][0]"/>
    			<field name="intimes" type="intimes_type" from="$result[0][1]"/>
    			<field name="begintime" type="begintime_type" from="$result[0][2]"/>
    			<field name="endtime" type="endtime_type" from="$result[0][3]"/>
    			<field name="sqlcode" type="sqlcode_type" from="$SQLCODE"/>
    		</responseParameter>
    	</message>

      sql 为该消息对应的一个活多个SQL语句；
      sql节点内可放置多个sql语句，每个语句可跨越多行； 配置在一个sql节点内的sql保证在同一个事务中完成;空行或行前后的空格会自动去除;
      要求每个sql的所有占位符都能找到对应入参，反过来不需要所有入参都需要映射到每个SQL语句;
      一次执行多条SQL特别适合有大量相同参数需要同时插入或更新到几个表的时候。
      内部所有的SQL都转换为prepareStatement执行;

      to 用来指定占位符 :1 为 占位符，用于和SQL对应; jvmdbbroker里可以随意使用字母数字，不要求是从1开始的数值， 另外如果占位符和入参的name相同(不区分大小写)，可不写to映射
      default 可为每个入参指定default，当没有传值则用该默认值;
      columnType 对数据库里为日期类型的入参，应设置columnType="date" (截除时分秒) 或 columnType="datetime", int,string不用指定, 默认为string

      入参的tlv类型只能是string, int

      如果sql是单条的insert/update/delete, 入参也可以是int array,或string array, 这种方式的后台处理是进行batch update, 对array里每条数据执行一次该sql, 所有执行都在一个事务里;
      batch update的时候，个别参数可以是单值，则每次执行sql都用相同值执行; 非单值则要求数组的大小必须相同;
      注意：batch update的时候目前的rowcount由于底层jdbc driver不支持返回的不一定准确，不应以此为判断依据，只应根据code来判断。

      $ROWCOUNT映射为返回查询结果记录数，对于更新操作，则是更新影响的行数(多条SQL则是总行数)；jvmdbbroker里只要name是rowcount或row_count(不区分大小写),可不写from
      $result[n][m]表示映射为结果集的第n行m列，一般单值都是映射到0行的数据; jvmdbbroker里只要name和select语句中的字段匹配(不区分大小写和顺序),则匹配为0行的对应值，可不写from, 如要匹配非0行数据，还是需要from

      在jvmdbbroker里，上述配置可简化为如下，仅仅是多了一个SQL语句：

    	<message name="QueryTimes" id="1">
    		<sql><![CDATA[
                    SELECT outtimes,intimes,begintime,endtime FROM TBSNDAIDBILLINGTIMESSET
                    WHERE sndaid=:1 and appid=:2 and areaid=:3 and rownum < 2
            ]]></sql>
    		<requestParameter>
    			<field name="sndaId" type="sndaId_type" />
    			<field name="appId" type="appId_type"  />
    			<field name="areaId" type="areaId_type" />
    		</requestParameter>
    		<responseParameter>
    			<field name="rowcount" type="rowcount_type" />
    			<field name="outtimes" type="outtimes_type"  />
    			<field name="intimes" type="intimes_type"  />
    			<field name="begintime" type="begintime_type"  />
    			<field name="endtime" type="endtime_type"  />
    			<field name="sqlcode" type="sqlcode_type"  />
    		</responseParameter>
    	</message>

    	更多映射方式：

      $SQLCODE为sql error code映射,jvmdbbroker里只要name是sqlcode(不区分大小写),可不写from
      $result[n]行映射 可将查询结果第n行映射到结构体, n一般是0, 如 <field name="row" type="row_type" from="$result[0]"/>
            对于结构体映射，要求结构体各参数顺序和select顺序完全一致，不支持按名字匹配
            jvmdbbroker里如果入参是一个struct类型且未写from, 则认为是要用结构体匹配第0行记录
      $result 整个结果集映射，通过 <field name="allrows" type="row_array_type" from="$result"/> 将整个查询结果集映射为一个结构体数组
            对于整个结果集映射，每条记录都对应一个map, 非常浪费内存，建议使用列映射
            jvmdbbroker里如果入参是一个struct array类型且未写from, 则认为是匹配所有记录
      $result[*][n] 将第n列(从0开始)映射到一个string array或int array; 这种方式内存少用很多，也不需要去定义结构体; 查大量数据建议用列映射
            jvmdbbroker里如果入参是一个string或int类型的array且未写from, 则按名字匹配：只要 select字段的名称 或加后缀（s, array, list, _array, _list）和入参匹配，则认为是要将该列匹配为一个数组类型的string或int

      SQL扩展，如果sql语句无法用:xxx这种的标记来表示，可改为使用$xxx标记，使用$xxx标记传入的字符串用来替换SQL, 如是值需带单引号; 用于动态的查询条件，排序条件等，$xxx这样的标记不限制数量；

      动态SQL替换 $dynamicSql, 是$xxx这种语法的一个用法，$dynamicSql不再是一个特殊标记，可换成任何合法字符串

    jvmdbbroker中允许的sql语句(以起始字符串判断): select,insert,update,merge,create,alter,drop,replace

    <DbThreadNum>16</DbThreadNum> 或  <dbthreadnum>16</dbthreadnum>

    配置db的默认线程数, 每一个DbSosList都会启动一组独立的线程池来执行任务, 而不是共用同一个线程池

    线程和连接基本配置:

    		<DbSosList threadNum="2">
    			<ServiceId>45601,...</ServiceId>
    			<MasterDb conns="2">
                    <DefaultConn>service=jdbc:oracle:thin:@10.241.37.37:1521:ebs user=riskcontrol password=des:7f4c1e7678077f51b7f9fb92f0d54023</DefaultConn>
    			</MasterDb>
    		</DbSosList>

    		threadNum未配置则用默认值
    		conns 为连接数
    		DefaultConn节点也可用defaultconn表示，兼容老的格式
    		DefaultConn中的service=可省略，兼容老的格式
    		service=后必须为一个有效的jdbc连接串, 支持计费这边常用的 url###url 模式
    		password可以用明文，也可用des:xxx表示des算法加密的密钥，加密方法同billing-dal中的算法, 可以为空串
    		内部连接池实现全部为固定连接数，不会上下浮动

    MYSQL的master/slave模式: 所有select先查slave,查失败，查master,非select直接用master

    		<DbSosList>
    			<ServiceId>45601,...</ServiceId>
    			<MasterDb conns="4" >
                    <DefaultConn>service=mysql_master_connect_string user=riskcontrol password=des:7f4c1e7678077f51b7f9fb92f0d54023</DefaultConn>
    			</MasterDb>
    			<SlaveDb conns="4" >
                    <DefaultConn>service=mysql_slave_connect_string user=riskcontrol22 password=des:7f4c1e7678077f51b7f9fb92f0d54023</DefaultConn>
    			</SlaveDb>
    		</DbSosList>

    		oracle下的RAC节点应用url1###url2的格式, 不可用此方式; 除非做了类似mysql的master/slave同步

  	分表

    		<DbSosList>
    			<ServiceId>45601,...</ServiceId>
    			<MasterDb conns="4" splitTableType="custom" tbfactor="6" splitTableCustomCls="jvmdbbroker.flow.SampleDbPlugin">
    			<DefaultConn>service=jdbc:oracle:thin:@10.241.37.37:1521:ebs###jdbc:oracle:thin:@10.241.37.37:1521:ebs user=riskcontrol password=des:7f4c1e7678077f51b7f9fb92f0d54023</DefaultConn>
    			</MasterDb>
    		</DbSosList>

		  	splitTableType 共有如下的分表方式：
		  	    assign 用分表参数值作为分表字段, 此为数据库的默认方式，可不配置
		  	    tail1 用分表参数的最后1位作为分表关键字
		  	    tail2 用分表参数的最后2位作为分表关键字
		  	    mod 用指定分表的hashCode取余作为分表关键字，需用tbfactor指定分表的数量
		  	    modpad0 用分表参数的hashCode取余作为分表关键字,不足2位前面加0，需用tbfactor指定分表的数量
		  	    custom 自定义 使用 splitTableCustomCls指定的类来生成分表关键字，该类需实现接口SplitTablePlugin, 实现中可任意实现自己的规则； 该实现类可直接放在流程文件中，不用单独写插件

		    分表时SQL语句的表名中需带分表关键字，如 insert into consume_switch_setting_$tableIdx(pt, consume_switch) values(:1, :2)
		        <field name="tableIdx" type="tableIdx_type"/>
		    $tableIdx也可用$hashNum代替;

  	分库

    		<DbSosList>
    			<ServiceId>45601,...</ServiceId>
    			<MasterDb conns="4" splitDbType="custom" dbfactor="3" splitDbCustomCls="jvmdbbroker.flow.SampleDbPlugin">
    			    <DivideConns>
    			      <Conn>service=jdbc:oracle:thin:@10.241.37.37:1521:ebs user=riskcontrol password=des:7f4c1e7678077f51b7f9fb92f0d54023</Conn>
    			      <Conn>service=jdbc:oracle:thin:@10.241.37.37:1521:ebs user=riskcontrol password=des:7f4c1e7678077f51b7f9fb92f0d54023</Conn>
    			      <Conn>service=jdbc:oracle:thin:@10.241.37.37:1521:ebs user=riskcontrol password=des:7f4c1e7678077f51b7f9fb92f0d54023</Conn>
    			    </DivideConns>
    			</MasterDb>
    		</DbSosList>

		   dbfactor必须等于conn数量，兼容参数
		   splitDbType 共有如下的分库方式：
		  	    no 不分库，默认
		  	    assign 用指定参数值作为分库字段, 要求请求中有dbHashNum或dbIdx入参，取值范围为(0 to conn-1)
		  	    custom 自定义 使用 splitDbCustomCls 指定的类来生成分库索引(0 to conn-1)，该类需实现接口SplitDbPlugin； 该实现类可直接放在流程文件中，不用单独写插件

  	如果有超大数量的查询，结果集未知，不能分页或限制行数，不能把结果集都放在内存后处理，这时可以使用

  	   QueryCallback接口，定义在dbplugin.scala文件中; 这个查询不依赖服务描述文件，不符合上述需求就不应该使用

22) 同步DB（支持事务）配置

    基本配置：

  		<SyncedDbSosList>
  			<ServiceId>45601,...</ServiceId>
  			<MasterDb conns="2">
                <DefaultConn>service=jdbc:oracle:thin:@10.241.37.37:1521:ebs user=riskcontrol password=des:7f4c1e7678077f51b7f9fb92f0d54023</DefaultConn>
  			</MasterDb>
  		</SyncedDbSosList>

    和异步DB的差异：

      1) 执行时线程不会发生切换，可以使用invoke调用该db服务下的各接口，但建议用 syncedInvoke调用, 一旦流程里使用invoke了"异步服务"，线程会发生切换，随后的调用必然出现异常
      2) 服务描述文件需增加3个特殊消息, 不需定义sql,requestParameter,responseParameter

         <message name="beginTransaction" id="10000"/>
         <message name="commit" id="10001"/>
         <message name="rollback" id="10002"/>

		  3) 显示使用syncedInvoke( "xxx.beginTransaction") 开始事务, 绑定连接
		  4) 显示使用syncedInvoke( "xxx.commit") 提交事务, 释放连接
		  5) 自动回滚: 执行该服务的任意消息出现异常都会自动回滚, 释放连接，不需要显示调用
		  6) 有需要可显示使用syncedInvoke( "xxx.rollback") 回滚事务;
		  7) 该服务的任何消息都需要在一个事务中，也就是必须先调用beginTransaction
		     通常将查询类的单独定义一个服务，对查询接口访问该异步DB服务，不用访问这个事务版本的服务
		  8) 不支持异步DB的Master/Slave, 不支持分库
		  9) SyncedDbSosList需和SyncedFlowCfg一起使用

23) AHT配置

    jvmdbbroker直接将AHT功能集成进来。

    服务描述文件差异：

        特殊的code:

      	<type name="signatureKey_type" class="string" code="10000" />
      	<type name="notifyUrl_type" class="string" code="10001" />

        如果签名密钥是动态，不是固定在配置文件中的，需使用10000配置确定哪个参数是签名密钥
        如果通知URL是动态，不是固定在配置文件中的，需使用10001配置确定哪个参数是通知URL

      	<message name="xxxx" id="1" signature="true">
      		<requestParameter>
      			<field name="notifyUrl" type="notifyUrl_type" />
      			<field name="signatureKey" type="signatureKey_type" />
                <field name="reqOrderNo" type="reqOrderNo_type" />
      			<field name="orderAmount" type="orderAmount_type" />
      			<field name="merchant_name" type="merchantName_type" default="youyun" />
      		</requestParameter>
      		<responseParameter>
      			<field name="resultCode" type="resultCode_type" path="return_code" isreturnfield="true" />
      			<field name="resultMsg" type="resultMsg_type" path="data.resultMsg" />
      			<field name="orderNo" type="orderNo_type" path="data.orderNo" />
      			<field name="payerPayTypeId" type="payerPayTypeId_type" path="data.payerPayTypeId" />
      			<field name="payerAmount" type="payerAmount_type" path="data.payerAmount" />
      		</responseParameter>
      	</message>

        signature属性表示该消息需要签名，jvmdbbroker支持放在AhtCfg配置NeedSignature里，建议放在config.xml里
        default 入参中可以使用default指定默认值
        path 响应和结果json的映射关系, 可用同样的方式配置从json,xml,form中解析结果，也可用jsonpath兼容老版本,
        isResultCode确定该字段是错误码字段，如未指定，则只要是HTTP 200，就认为code=0，否则报错; 也兼容isreturnfield配置参数

    config.xml中的配置

  		<AhtCfg>

          <ServiceId>980,981</ServiceId>

          <TimeOut>15</TimeOut>
          <ConnectTimeout>3</ConnectTimeout>
          <ThreadNum>2</ThreadNum>
          <TimerInterval>100</TimerInterval>

        	<Service>

        		<Item>
        			<ServiceId>9940</ServiceId>
        			<MsgId>-1</MsgId>
        			<NeedSignature>true</NeedSignature>
        			<Signature>8eda05629c9f4fee3bcdfa826562c45c</Signature>
        			<Method>GET</Method>
        			<RequestContentType>application/x-www-form-urlencoded</RequestContentType>
        			<CharSet>UTF-8</CharSet>
        			<ServerUrl>http://xxx:xxxx/sdpp/payment/mobile/prepare</ServerUrl>
                    <WSSOAPAction>http://tempuri.org/IDepositOrder/CheckDepositOrder</WSSOAPAction>
                    <WSReqSuffix></WSReqSuffix>
                    <WSResSuffix>Response</WSResSuffix>
                    <WSReqWrap>orderInfo</WSReqWrap>
                    <WSResWrap>CheckDepositOrderResult</WSResWrap>
                    <WSNs>ns2=http://tempuri.org/ default=http://schemas.datacontract.org/2004/07/Sdo.Service.PayOrder.Entity</WSNs>
                    <WSWrapNs>ns2</WSWrapNs>
        			<Plugin>xxx</Plugin>
        		</Item>

        		<Item>
        			<ServiceId>9940</ServiceId>
        			<MsgId>1</MsgId>
        			...
        			<ServerUrlSuffix>...</ServerUrlSuffix>
              ...
        		</Item>

        		...

        	</Service>

  		</AhtCfg>

      TimeOut HTTP数据发送超时时间, 默认为15秒，以秒为单位
      ConnectTimeout HTTP连接超时时间, 默认为3秒，以秒为单位
      ThreadNum Aht Actor 使用的线程数，默认为2个
      TimerInterval 内部定时器的间隔毫秒数，默认为100毫秒，如果需要100毫秒内的超时精度，可调整此值
      TimeOut,ConnectTimeout,ThreadNum,TimerInterval的设置也可以用AhtCfg节点的属性方式设置，对应属性：timeOut,connectTimeout,threadNum,timerInterval

      AHT默认支持的格式：
          请求 application/x-www-form-urlencoded
          响应 json, xml, application/x-www-form-urlencoded,text/html,text/plain
            对xml，path中除了a.b 这种，还可用 a.b@xxx 取a节点下的b节点的xxx属性
            对text/html,text/plain，根据响应的第一个字符判定内容格式，<则认为是xml, {则认为是json
          不符合上述规则的则需实现一个插件类
      AHT签名方法：
          默认支持的签名方法是HPS签名方法，如果需要其它的签名方法需实现一个插件类

      配置为按消息配置，也可用-1配置所有消息的默认值, 没有为消息配置则取-1的配置

      NeedSignature 该消息是否需要签名，可用来替代msg的signature属性
      Method get还是post 默认为post
      RequestContentType 请求类型, 默认为form
      CharSet 默认为 utf-8
      Signature 签名密钥, 如消息中定义了code=10000的入参，则以入参为准
      ServerUrl 通知地址, 如消息中定义了code=10001的入参，则以入参为准
          如果非-1的消息里配置了ServerUrlSuffix, 则实际通知url是ServerUrl + ServerUrlSuffix, 可用来将前缀相同部分都配置在-1节点
					ServerUrl支持http或https的url

      在Method为POST, RequestContentType为text/xml时，则进入web service调用模式

        aht插件不使用第三方webservice库来调用远程服务，而是直接拼出符合要求的xml发给远程webservice服务

        WSNs 请求中所有用到的namespace定义 n1=v1 n2=v2形式，用空格隔开多个定义，第一个namespace也作为方法节点的namespace, 默认节点用default表示
        WSReqSuffix 请求根节点的后缀，全名为方法名+后缀, 此值一般和方法名一样，不用配置, 此节点的ns为WsNs的第一个值
        WSResSuffix 响应根节点的后缀，全名为方法名+后缀，此值一般是Response, 此节点的ns为WsNs的第一个值
        WSWrapNs 请求和响应包装节点对应的namespace, default表示默认namespace, 也可不配置
        WSReqWrap 请求包装节点的名称，可为空串, 此值可能随每个方法不一样，比如arg0等
        WSResWrap 响应包装节点的名称，可为空串, 此值可能随每个方法不一样，比如return等
        WSSOAPAction 方法对应的soapaction，有些webservice需要配置此值，有些不需要, 此值可能随每个方法不一样

      webservice的服务描述文件

        每个field都可以额外指定一个ns参数来指定namespace, 可以为空
        如果field名称是用.分隔，表示是嵌套xml; 如需指定ns, 必须为每个级别指定ns，如key为a.b, 则ns为"ns1.ns2", 若为*表示用默认namespace
        方法名，field名都和xml一一对应，大小写必须一致
        参数的顺序和xml的顺序一致，如果错乱会导致服务端返回500错误

      Plugin 指定插件实现类的类名, 该插件可在流程文件中，不需单独jar包

	      如果指定了插件类，则会用插件类替代默认实现

	      1) 标记接口 该插件需实现 trait HttpPlugin {} 并可选实现下面的2,3,4中一个或多个

        2) 签名插件

            trait HttpSignPlugin {
              def sign(msg:HttpMsgDefine,signatureKey:String,body:HashMapStringString):Unit
            }

            可自行生成签名字段加到body中然后继续发送

        3) 请求处理插件

            trait HttpRequestPlugin {
              def generateRequestBody(msg:HttpMsgDefine,body:HashMapStringAny):String
            }

            可自行根据body生成待发送的内容;
            通常实现了此接口就包括了签名，不需要再实现HttpSignPlugin

        4) 响应处理插件

            trait HttpResponsePlugin {
              def parseContent(msg:HttpMsgDefine, contentStr:String, body: HashMapStringAny ): Int
            }

            可自行解析contentStr到body中

