# <a name="toc">目录</a>

[Avenue协议](#avenue)

[服务描述文件](#service)

[FLOW文件格式](#flow)

[启动时编译](#compile)

[Scala语法和Java语法对比](#compare)

# <a name="avenue">Avenue协议</a>

## 介绍

  avenue协议是自定义的TCP长连接协议，ScalaBPE框架使用此协议和其它服务通讯

## 请求格式

    
    0........ 8........16........24........32
    1  |--0xA1---|headLen-|--version-|----M---| //协议标记,头长,版本号,路由数
    2  |---------------packetLen--------------| //包长 
    3  |---------------serviceId--------------| //服务id
    4  |----------------msgId-----------------| //请求id
    5  |---------------sequence---------------| //序列号
    6  |---------------optional---------------| //可选标记位
    7  |---------------priority---------------| //优先级
    8  |-------------signature----------------|
    9  |-------------signature----------------|
    10 |-------------signature----------------|
    11 |-------------signature----------------| //16字节签名
    
    headLen 填44 + 扩展包头长度
    
    version 固定填1, 
    M 固定填0x0F
    packageLen 填实际包长
    
    serviceid   服务编号
    msgid 消息编号
    
    serviceid,msgid 都为0表示心跳，这时无body
    
    sequence为序列号
    
    optional    标志位：
    
        context 固定填0 
        mustReach 必达标志位 填0表示一般的包，填1表示必达包
        format 填 0 表示 tlv, 填 1 表示 json, 目前仅用到0
        encoding 填 0 表示 gbk, 填 1 表示 utf-8
    
    priority： 0 一般 1 高 实际未用到
    
    signature 全部填0
    
    body: 格式和编码取决于format和encoding

## 响应格式
        
    使用标准的Avenue协议包头, 无扩展包头: 

    0........ 8........16........24........32
    1  |--0xA2---|headLen-|--version-|reserve-| //协议标记，头长，版本号，保留位
    2  |---------------packetLen--------------| //包长 
    3  |---------------serviceId--------------| //服务id
    4  |----------------msgId-----------------| //请求id
    5  |---------------sequence---------------| //序列号
    6  |---------------optional---------------| //可选标记位
    7  |-----------------code-----------------| //响应码
    8  |-------------signature----------------|
    9  |-------------signature----------------|
    10 |-------------signature----------------|
    11 |-------------signature----------------| //16字节签名
    
    其中：
    
    headLen 填44 + 扩展包头长度
    
    version 固定填1
    reserve 固定填0
    packetLen 填实际包长; 对心跳包和ack包，固定式44
    
    serviceid   服务编号
    msgid 消息编号
    
    serviceid,msgid 都为0表示心跳，这时无body
    
    sequence为序列号,需和请求中的一致
    
    optional    标志位：
    
        context 固定填0 
        mustReach 必达标志位 对响应包忽略
        format 填 0 表示 tlv, 填 1 表示 json
        encoding 填 0 表示 gbk, 填 1 表示 utf-8
    
    code：
    
        填0表示正常响应
        填100表示ack响应包
        其它值为错误码
    
    signature 全部填 0
    
    body: 格式和编码取决于format和encoding

[返回](#toc)

# <a name="service">服务描述文件</a>

## 介绍

所有外部服务（访问数据库，操作缓存，调用外部http服务，调用Avenue服务等）都通过服务描述文件来抽象。
流程中调用所有的外部服务都是以相同的方式来调用。

示例:  参考 avenue_conf/flow999.xml

## avenue_conf目录结构约定

__建议将对外的服务描述文件直接放在avenue_conf下__

__而内部使用的服务描述文件则放在子目录下__

子目录可以采用 internal/external的命名方式，也可按服务名来命名，根据自己的需要决定

## service定义

    name 服务名，不区分大小写, 流程中使用此名称
    id 服务号, 不可以和其它服务的ID重复，网络传输时传输此值

## type定义

    name 名称，不区分大小写，建议以_type结尾, 在同一个服务描述文件中必须唯一

    code 对应的tlv编码值，在同一个服务描述文件中必须唯一, 网络传输时传输此值

    class 共有4种:

        int 整数
        string 字符串， 其长度最大为64K (一个short), long,float,double等类型都必须转成string
        struct 对象类型, 每个struct由多个field组成
        array 数组类型

        注：二进制类型数据不是一个单独的class, 而是特殊的string, 设置了isbytes为1的string则表示是二进制内容

    default 默认值

    * struct里的field定义

        name 对象的名称，__区分大小写__, 流程中使用此名称
        type 类型
          int 整数
          systemstring 字符串, 无需指定长度
          string 字符串, 需通过len定义长度, 若是最后一个field，可不指定len，此类型主要是为兼容老版本
        default 默认值

    * array定义

        itemType 该数组每个元素对应的type的name

    * 服务描述文件中的类型和ScalaBPE数据类型的对应关系

        int             scala.Int
        bytes           scala.Array[scala.Byte]
        string          java.lang.String
        struct          jvmdbbroker.core.HashMapStringAny 继承自 scala.collection.mutable.HashMap[String,Any]
        int array       jvmdbbroker.core.ArrayBufferInt 继承自 scala.collection.mutable.ArrayBuffer[Int]
        string array    jvmdbbroker.core.ArrayBufferString 继承自 scala.collection.mutable.ArrayBuffer[String]        
        struct array    jvmdbbroker.core.ArrayBufferMap 继承自 scala.collection.mutable.ArrayBuffer[jvmdbbroker.core.HashMapStringAny]

## message 定义

    name 消息名称，不区分大小写，流程中使用消息名来调用
    id 消息ID, 在同一个服务描述文件中必须唯一, 网络传输时传输此值
    isAck 该消息是否为必达消息，参见必达消息的说明
    retryInterval 重试间隔，毫秒， isAck为true时使用
    retryTimes 重试次数，isAck为true时使用

    requestParameter 子节点 定义请求参数，由0-n个field组成
    responseParameter 子节点 定义响应参数，由0-n个field组成

    field定义

        name 参数名，__此值区分大小写__，流程中使用此名称
        type 类型名，不区分大小写
        default 默认值

## 服务描述文件扩展

    由于所有服务提供的接口都通过服务描述文件来描述，标准的服务描述文件格式不能满足需求。
    每个插件会根据需要对服务描述文件做一些扩展或约定，如:
        
        缓存服务要求的 get 方法的 id必须是 1, set方法的id必须是2
        DB服务在每个message下增加了一个sql节点来定义sql语句

    这类扩展参见各个插件的配置文档。

## 默认值 default value

    default系列标签可以出现在<type>内 或者请求/响应中的<field>内 或 struct里的 <field>内, 请求响应中的配置优先于type上的配置
    未设置default属性和设置default=""含义不同，一个表示NULL,一个表示空串; 只有入参为null时，default值才会起作用

## validator

    validator系列标签可以出现在<type>内 或者请求/响应中的<field>内 或 struct里的 <field>内, 请求响应中的配置优先于type上的配置

    validator系列标签有3个：validator 、validatorParam、returnCode分别表示验证名、验证参数、验证失败后的返回码

    若请求字段校验失败，直接返回错误码。若响应字段校验失败，包体不变，包头code为修改为错误码。原响应包code!=0时不触发校验。

        大类  validator    validatorParam      参数说明        returnCode      实现说明

        必填  Required      不需要             不需要           默认-10242400  用于判断必填，其中空字符串算做有值
        正则类 Regex        配置为正则表达式   是否符合正则     默认-10242400  最基础的validator
                Email       不需要             不需要           默认-10242400  通过正则内置实现，等价于正则参数：([0-9A-Za-z\\-_\\.]+)@([a-zA-Z0-9_-])+(.[a-zA-Z0-9_-])+
                Url         不需要             不需要           默认-10242400  通过正则内置实现
        范围类  NumberRange 数字1,数字2        左闭由闭区间     默认-10242400  用于判断整数范围
                LengthRange 数字1,数字2        左闭由闭区间     默认-10242400  用于判断字符串长度范围
                TimeRange   字符串1,字符串2    左闭由闭区间     默认-10242400  用于判断时间范围 示例：2011-1-2 13:00:05.231,2018-2-3 15:00:00.345
        集合类  NumberSet   A|b|C|d|e|…                         默认-10242400  用于整数枚举类型，示例 0|1
                Regex       A|b|C|d|e|…                         默认-10242400  完全同正则validator，普通正则实现

## encoder

    encoder系列标签可以出现在<type>内 或者请求/响应中的<field>内, 或者struct里的<field>内, 请求响应中的配置优先于type上的配置

    encoder系列标签有2个：encoder、encoderParam分别表示编码名、编码参数

    encoder对请求、响应均有效

    实现的Encoder有：

        encoder             encoderParam          参数说明      实现说明

        NormalEncoder     A,b|c,d|<,&lt         |是大分割符，逗号是小分隔符，代表将A转义为b,将c转义为d, |,\三个字符实现为关键字，要输入实际这三个字符使用\转义，比如\|   \,  \\
        HtmlEncoder         无                     无            基于NormalEncoder实现，等价于： &,&amp;|<,&lt;|>,&gt;|",&quot;|',&#x27;|/,&#x2f;
        HtmlFilter          无                     无            基于NormalEncoder实现，等价于： &,|<,|>,|",|',|/,|\\,
        NocaseEncoder       无                     无            不区分大小写的NormalEncoder编码转换
        AttackFilter        无                     无            基于NocaseEncoder,等价于： script,|exec,|select,|update,|delete,|insert,|create,|alter,|drop,|truncate,|&,|<,|>,|",|',|/,|\\,

## 必达消息说明

    <message name="testbatchupdate" id="7" __isAck="true" retryInterval="30000" retryTimes="100"__>

      isAck (或者isack) 是否要将一个消息设置为必达消息，默认为false
      retryTimes 重试次数，默认为 3*24*60, 按一分钟一次，要尝试3天
      retryInterval 重试间隔时间，默认为1分钟

      可通过<Parameter name="mustReach.defaultRetryTimes">xxx</Parameter>调整默认值
      可通过<Parameter name="mustReach.defaultRetryInterval">xxx</Parameter>调整默认值

    必达消息的语义：如果一个请求的返回码是以下3个错误码则认为没有"必达"，需要进行重试。

        网络错误  -10242404
        服务或消息未找到 -10242405
        队列满或队列阻塞引起的超时 -10242488

    目前sos,db,cache等插件都可能会返回以上错误码。

    必达消息可以在任何服务描述文件中定义，而不仅仅在对外的服务的描述文件。
    可以定义在一个子流程上，也可以在某个db的消息上。由业务根据实际情况使用。
    在接收到客户端的必达消息时，只要一完成消息的持久化，就会先回一个ACK消息给客户端，jvmdbbroker自己会重试，不需要客户端再重试。

    为兼容老版本，如果发现avenue包头设置了"必达消息位"，即使服务描述文件中未申明也会作为必达消息处理

    必达消息的持久化数据保存在data/mustreach目录下，以服务名_消息名为队列名。

[返回](#toc)

# <a name="flow">FLOW文件格式</a>

## 目录结构

compose_conf 目录下可以有2种文件：scala文件和flow文件

__建议将scala后缀的文件直接放在compose_conf下__

__而flow文件则根据服务名分别放在对应服务名的子目录下__

__flow文件的命名建议用 消息名_消息号.flow 的格式__

## 文件类型

    .scala后缀的纯scala文件, 一般是一些辅助类，轻量级插件类等, 也可以直接写流程
    对.scala后缀的类文件，包名建议统一用jvmdbbroker.flow，简化import语句

    .flow结尾的流程文件, 每个flow就是服务描述文件中的一个消息的实现
    每个flow文件在编译时会自动转换成scala类，编写flow文件，实际就是写scala类
    所以scala的语法在flow里可以没有限制的使用
    所有flow文件的包名会自动设置为jvmdbbroker.flow, 并自动加上对jvmdbbroker.core的import

## flow文件内的特殊标记

    * 消息申明：//$service999.updateUserInfo

      说明：//$指定该流程对应的服务名和消息名, 此行不一定要是第一行，前面可以有import语句
            流程和消息是通过//$后的申明来对应的，而和该flow的文件名没有关系
            申明时服务名和消息名不区分大小写


    * 入口地址：//#receive

      说明：每个流程都有一个入口地址, 流程从该入口开始执行


    * 回调函数地址：//#nnn

      说明：在流程中每个异步调用都需要指定一个回调函数，在异步请求完成后框架会自动调用该函数
            继续流程， 回调函数名可以自己随便定义, 回调函数可以有任意多个，根据自己需要定义

          在流程中通过invoke系列方法进行异步调用，如：

              invoke(querycallback,3000,"userId"->"123")

          这里 querycallback 就是一个回调函数

## 流程的基类

      默认流程都继承类jvmdbbroker.core.Flow, 使用特殊语法可以修改继承的基类

      使用//$xxx.xxx.withsyncedinvoke语法可以让流程继承jvmdbbroker.core.SyncedFlow, 
      继承后可以使用syncedInvoke系列函数, 在支持数据库事务的流程中使用此方法会更方便

      使用//$xxx.xxx.with(baseclass)语法可以让流程继承一个用户自定义的基类baseclass,
      baseclass必须继承jvmdbbroker.core.Flow, 用户可以在基类中做一些特殊处理，
      如在每个流程开始前自动从redis中加载http的session信息到内存中的session变量中，
      流程中只要操作session变量就能读写会话信息

[返回](#toc)

# <a name="compile">启动时编译</a>

    框架在启动时会对compose_conf下的文件进行编译
    如果scala文件或flow文件有编译错误，会给出精确的行号以便快速定位

    每个flow文件在启动的时候会转换为scala类文件，转换后的文件保存在temp/src目录下
    flow转换完毕后再编译scala文件，所有scala文件会编译成.class文件再启动程序

    编译过程比较耗时，为加快启动速度，scalabpe使用增量编译，规则如下：

    任意一个lib下的jar文件时间戳有改变，需要全量编译
    任意一个scala文件的时间戳有改变，需要全量编译
    如仅仅是flow文件变化，使用增量编译，只编译改动的flow文件

    所以：

    1) 所有的流程用flow文件，不要用scala文件，scala文件里只写公共的辅助类或插件
    2) flow文件中可以定义类，但一定不要是多个flow共享的，如果多个flow共享，用scala文件

[返回](#toc)

# <a name="compare">Scala语法和Java语法对比</a>

    feature                                 java                                                              scala
    ======================================================================================================================================================

    类型申明                                int a; String b;                                                  val a:Int=0; var b:String="abc";
    val申明的变量                           相当于final对象，不可再赋值，必须初始化                           val a=2;  不可再赋值 ; scala里建议尽量用val
    var申明的变量                           相当于非final变量，可再赋值                                       var a=3;  a=4;

    行尾的分号                              必须                                                              可省略; 一行多个语句时不能省略
    无参数方法调用                          a.xxx()                                                           可省略成a.xxx
    非void函数return关键字                  必须写                                                            一般函数最后的return能省略的都省略，最后一个表达式的值就是返回值; 函数中间过程的return不能省略
    函数默认值                              无                                                                可以，可减少重复的函数定义
    Getter/Setter                          一般都使用bean此风格申明类                                         scala里不使用java bean风格, 直接引用变量
    类型推导                                无                                                                val a = 1;  a则是Int; val b = "" 则b是String;
                                                                                                              val c = null;  这时c类型不明确，一般用 val c:String = null 或者 val c:String = _ 或者 val c = null:String
                                                                                                              凡是能推导出类型的地方都可不写类型
    函数定义                                int add(int a,int b) { return a + b }                             def add(a:Int,b:Int): Int = { a + b } 根据是否能自动推导，可有多个变体:
                                                                                                              def add(a:Int,b:Int) = { a + b }  Int可自动推导出
                                                                                                              def add(a:Int,b:Int) = a + b  还可省略大括号, 注意=不能省略；没有=号的函数认为返回Unit,



    VOID类型                                void                                                              Unit
    int类型                                 int                                                               scala.Int
    string类型                              java.lang.String                                                  相同，也是java.lang.String
    字符串/int转换                          Integer.parseInt(s),                                              s.toInt, java.lang.String没有这个方法，scala编译器会转换
                                            String.valueOf(i)                                                 i.toString

    对象比较                                equals(),!equals()                                                == !=
    对象==                                  ==                                                                eq 一般不用

    泛型                                    <>                                                                []
    数组下标                                []                                                                ()

    import                                  文件开头                                                          文件开头，类内(仅在该类内有用)，函数内(仅在该函数内有用)
    import所有类                            import java.util.*                                                import jvmdbbroker.core._; import jvmdbbroker.plugin.{SplitTablePlugin,SplitDbPlugin}; 一行引入多个类

    ?:表达式                                int a = ok ? 1 : 0;                                               val a = if ok 1 else 0
    ++运算符                               有   ++i                                                           用 i += 1 代替

    条件判断                                if ... else if ... else                                           相同
    while, do while                         while, do while                                                   相同
    循环中break,continue                    有                                                                无
    for循环1                                for(;;)                                                           不支持
    for循环2                                for( a <- b )                                                     有，功能很强，不是传统的循环，不做详细说明;
    switch                                  switch { case ... case ... other ... } case后需要break            xxx match {case ... } 非常非常强大，不做详细说明, 注意case后不需要break
    异常                                    try { ... } catch(Exception e) { ... } finally {...}              try { ... } catch { case a:Exception => ... } finally {...} 也是用match的case语法
    异常catch                               非runtime exception需要catch                                      不需要
    常用数据结构                            List,Set,Map                                                      java的类库都可使用，但一般情况下用scala自己的集合类库，不能满足需求再用java类库的，scala常用：
                                                                                                                scala.collection.mutable.ArrayBuffer  功能等价于java的ArrayList，加数据可以用  buff += a
                                                                                                                scala.collection.mutable.HashMap 功能等价于java的HashMap  注意其中的get和java的不一样，用getOrElse(key,defaultValue)才是java里的get
                                                                                                                scala.collection.mutable.HashSet 功能等价于java的HashSet
    定长数组                                new String[3]                                                     new Array[String](3)  固定大小数组，动态数组用ArrayBuffer[String]
    Tuple                                   无                                                                Tuple2,Tuple3,... 对象，非常好用
    多重赋值                                不支持                                                            val (ret1,ret2,...) = xxx, 可使用此方法从Tuple中直接取值, jvmdbbroker flow使用这个从并行调用中获取结果
    列表LIST                                无                                                                scala里的List和java的List没任何关系，也完全不是一回事; 主要是用来做函数式编程的

    对象根                                  Object                                                            Any, 一切都是对象; Object是Any的一个子类
    类的静态成员                            static                                                            class 内都是实例成员, 静态成员放在一个同名的object单例对象内; 也可单独定义没有class的object对象
    接口申明                                interface                                                         trait 可有具体实现代码
    抽象类                                  abstract class                                                    trait
    类继承                                  只能继承(extends)一个类，实现(implements)多个接口                 只能继承(extends)一个类, 但可混入(with)多个trait, 此设计非常强大
    函数也是对象                            不支持                                                            支持,jvmdbbroker里invoke的callback就是函数; java中只能传递字符串然后通过反射找到对应方法
    类申明                                  class  SocRequest { ... }                                         可同时申明实例成员, 可带默认值  class SocRequest (
                                                                                                                val requestId : String,
                                                                                                                    val serviceId : Int,
                                                                                                                    val msgId : Int,
                                                                                                                    val body : HashMapStringAny,
                                                                                                                val encoding : Int = AvenueCodec.ENCODING_UTF8,
                                                                                                                    val xhead : HashMapStringAny = new HashMapStringAny() ) { ... } 就申明一个类，里面有requestId,serviceId,msgId,body,encoding,xhead几个实例变量。
    字符串格式化                            String中没有                                                      直接使用String类的format方法 "s=%s d=%d".format(s,d) 方法来格式化字符串，类似c语言的format格式
    map.get()                               String s = map.get("abc")                                         val s = map.getOrElse("abc",null) scala里HashMap.get返回的是Option对象，一般不用


[返回](#toc)
