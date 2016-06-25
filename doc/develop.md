# 开发指南



26) 增量编译

    编译有些耗时，可能需要10多秒，为加快启动速度，jvmdbbroker使用增量编译，规则如下：

    lib/*.jar 时间戳有任何改变，需要全量编译
    compose_conf/*.scala 任意一个文件的时间戳有任何改变，需要全量编译
    compose_conf/*.flow 如仅仅是flow文件变化，使用增量编译，只编译改动的flow文件

    所以：

    1) 所有的流程用flow文件，不要用scala文件，scala文件里只写公共的辅助类
    2) flow文件中可以定义类，但一定不要是多个flow共享的，如果多个flow共享，用scala文件




30) syncedInvoke特别说明

    1) 要使用此方法需要withsyncedinvoke特殊标记， //$xxx.xxx.withsyncedinvoke
    2) 只应该使用syncedInvoke调用本地缓存或同步DB服务
    3) syncedInvoke调用本地缓存或同步DB服务时，timeout参数传0
    4) 调用本地缓存用invoke也可同样实现，只是需要多一次跳转，可用invokeN一次查询多个配置，就只需要一次跳转
    5) syncedInvoke同步DB服务时需配置独立的SyncedFlowCfg线程池
    6) 调用子流程：

          invoke 子流程是异步调用，必须设置 timeout > 0 才能拿到返回值，这是常用的子流程共享
          syncedInvoke 子流程是同步调用，如子流程内未再发生invoke, 线程就不会切换，可设置 timeout = 0, 一定可以拿到返回值，可用于事务内的子流程共享
          但是如果syncedInvoke的子流程内又有异步调用, 则必须设置 timeout > 0才能拿到返回值

31) 必达消息说明

    <message name="testbatchupdate" id="7" isAck="true" retryInterval="30000" retryTimes="100">

      isAck (或者isack) 是否要将一个消息设置为必达消息，默认为false
      retryTimes 重试次数，默认为 3*24*60, 按一分钟一次，要尝试3天
      retryInterval 重试间隔时间，默认为1分钟

      可通过<Parameter name="mustReach.defaultRetryTimes">xxx</Parameter>调整默认值
      可通过<Parameter name="mustReach.defaultRetryInterval">xxx</Parameter>调整默认值

    必达消息的语义：如果一个请求的返回码是以下3个错误码则认为没有"必达"，需要进行重试。

        超时  -10242504
        网络错误 -10242404
        队列满或开始处理请求时发现请求已超时 -10242488

    目前sos,db,cache等都会返回上述错误码。

    必达消息可以在任何服务描述文件中定义，而不仅仅在对外的服务的描述文件。
    可以定义在一个子流程上，也可以在某个db的消息上。由业务根据实际情况使用。
    在接收到客户端的必达消息时，只要一完成消息的持久化，就会先回一个ACK消息给客户端，jvmdbbroker自己会重试，不需要客户端再重试。

    为兼容老版本，如果发现avenue包头设置了"必达消息位"，即使服务描述文件中未申明也会作为必达消息处理

    必达消息的持久化数据保存在data/mustreach目录下，以服务名_消息名为队列名。


34) 服务描述文件里的default value, validator, encoder

default value 定义为：

1、	default系列标签可以出现在<type>内 或者请求/响应中的<field>内 或 struct里的 <field>内, 请求响应中的配置优先于type上的配置
2、	未设置default属性和设置default=""含义不同，一个表示未NULL,一个表示未空串; 只有入参为null时，default值才会起作用

--------------

validator定义为：

1、	validator系列标签可以出现在<type>内 或者请求/响应中的<field>内 或 struct里的 <field>内, 请求响应中的配置优先于type上的配置
2、	validator系列标签有3个：validator 、validatorParam、returnCode分别表示验证名、验证参数、验证失败后的返回码
3、	若请求字段校验失败，直接返回错误码。若响应字段校验失败，包体不变，包头code为修改为错误码。原响应包code!=0时不触发校验。

大类	validator	validatorParam	    参数说明	    returnCode	    实现说明

必填	Required    不需要	            不需要	        默认-10242400	用于判断必填，其中空字符串算做有值
正则类	Regex	    配置为正则表达式	是否符合正则	默认-10242400	最基础的validator
        Email	    不需要	            不需要	        默认-10242400	通过正则内置实现，等价于正则参数：([0-9A-Za-z\\-_\\.]+)@([a-zA-Z0-9_-])+(.[a-zA-Z0-9_-])+
        Url	        不需要	            不需要	        默认-10242400	通过正则内置实现
范围类 	NumberRange	数字1,数字2	        左闭由闭区间	默认-10242400	用于判断整数范围
        LengthRange	数字1,数字2	        左闭由闭区间	默认-10242400	用于判断字符串长度范围
        TimeRange	字符串1,字符串2	    左闭由闭区间	默认-10242400	用于判断时间范围 示例：2011-1-2 13:00:05.231,2018-2-3 15:00:00.345
集合类 	NumberSet	A|b|C|d|e|…	                     默认-10242400	 用于整数枚举类型，示例 0|1
        Regex	    A|b|C|d|e|…		                 默认-10242400 	完全同正则validator，普通正则实现

--------------

encoder定义为：

1、	encoder系列标签可以出现在<type>内 或者请求/响应中的<field>内, 或者struct里的<field>内, 请求响应中的配置优先于type上的配置
2、	encoder系列标签有2个：encoder、encoderParam分别表示编码名、编码参数
3、	encoder对请求、响应均有效
4、	拟实现的Encoder有：

encoder	            encoderParam	        参数说明	    实现说明

NormalEncoder	    A,b|c,d|<,&lt	        |是大分割符，逗号是小分隔符，代表将A转义为b,将c转义为d, |,\三个字符实现为关键字，要输入实际这三个字符使用\转义，比如\|   \,  \\
HtmlEncoder	        无	                    无	        基于NormalEncoder实现，等价于： &,&amp;|<,&lt;|>,&gt;|",&quot;|',&#x27;|/,&#x2f;
HtmlFilter	        无	                    无	        基于NormalEncoder实现，等价于： &,|<,|>,|",|',|/,|\\,
NocaseEncoder       无	                    无	        不区分大小写的NormalEncoder编码转换
AttackFilter        无	                    无	        基于NocaseEncoder,等价于： script,|exec,|select,|update,|delete,|insert,|create,|alter,|drop,|truncate,|&,|<,|>,|",|',|/,|\\,



