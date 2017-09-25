# <a name="toc">目录</a>

[Scala/Java语法对比](#compare)

[服务描述文件](#service)

[流程文件格式](#flow)

[流程编译](#compile)

[流程编写基础知识](#writeflow)

[流程中如何实现循环](#loop)

[Json转换](#json)

[本地存储](#localstorage)

[自定义启动和关闭函数](#global)

[实现自定义的流程基类](#baseflow)

[Avenue协议](#avenue)

[TLV编码规范](#tlv)

# <a name="compare">Scala/Java语法对比</a>

| feature | java | scala |
| ------- | ---- | ----- |
|类型申明|int a; <br>String b; |var a=0; <br>var b="";|
| val申明的变量 | 相当于final对象，不可再赋值 | val a=2; 必须初始化,不可再赋值 |
| var申明的变量 | 相当于非final对象，可再赋值 | var a=3;  a=4; |
| 行尾的分号 | 必须 | 可省略; 一行多个语句时不能省略 |
| 无参数方法调用 | a.xxx()  | 可省略成a.xxx |
| 非void函数return关键字 | 必须写 | 一般函数最后的return能省略的都省略，最后一个表达式的值就是返回值; 函数中间过程的return不能省略 |
| 函数参数默认值 | 无 | 可以，可减少重复的函数定义 |
| Getter/Setter | 一般都使用bean此风格申明类 | scala里不使用java bean风格, 直接引用变量 |
| 类型推导 | 无 | 凡是能推导出类型的地方都可不写类型<br>val a = 1;  a则是Int; <br>val b = "" 则b是String;<br>val c = null;  这时c类型不明确，一般用 val c:String = null 或者 val c:String = _ 或者 val c = null:String |
| 函数定义 | int add(int a,int b) { return a + b }  | def add(a:Int,b:Int): Int = { a + b } 根据是否能自动推导，可有多个变体:<br>def add(a:Int,b:Int) = { a + b }  Int可自动推导出; <br>def add(a:Int,b:Int) = a + b  还可省略大括号, 注意等号不能省略； |
| VOID类型 | void | Unit |
| int类型 | int | scala.Int |
| string类型 | java.lang.String | 相同，也是java.lang.String |
| 字符串中 | 只有 "..." | 可以用"...", <br>也可用 """...""", """包围的字符串里特殊字符不用转码 |
| 字符串格式化 | String中没有 | 直接使用String类的format方法 "s=%s d=%d".format(s,d) 方法来格式化字符串，类似c语言的format格式 |
| 字符串/int转换 | Integer.parseInt(s)<br>String.valueOf(i) | s.toInt, java.lang.String没有这个方法，scala编译器会转换<br>i.toString|
| 对象比较 | equals()<br>!equals() | ==<br>!= |
| 对象== | == | eq 一般不用 |                                                              
| 泛型 | <> | [] |                                                              
| 数组下标 | [] | () |                                                              
| import | 文件开头 | 文件开头<br>类内(仅在该类内有用)<br>函数内(仅在该函数内有用) |                                                              
| import所有类 | import java.util.* | import java.util._;<br>import java.util.{ArrayList,HashMap}; <br>可一行引入多个单独类 |                                                              
| ?:表达式  | int a = ok ? 1 : 0; | val a = if ok 1 else 0 |                                                              
| ++运算符 |  ++i 或 --i  | 用 i += 1 代替 |
| 条件判断 | if ... else if ... else | 相同 |
| while, do while | while, do while | 相同 |
| for循环1 | for(;;) | 不支持 |
| for循环2 | for( a <- b ) | 有，功能很强，不是传统的循环，scala的for很强大! |
| 循环中break | 有 | 无此关键字, 但可用<br>import scala.util.control.Breaks._;  <br>breakable { ... } 来实现 |
| 循环中continue | 有 | 无此关键字 |
| switch | switch {<br>case v: ... <br>case v: ... <br>default ... <br>} <br>每个case后需要break | xxx match { <br>case ... => ... ; <br>case ... => ...; <br>case _ => ...; <br>} <br>case 后不需要break, <br>scala的match非常非常强大!!|
| 异常 | try { ... } <br>catch(Exception1 e) { ... } <br>catch(Exception2 e) { ... } <br>finally { ... } | try { ... } catch { <br>case a:Exception1 => ... <br>case a:Exception2 => ... <br>} finally {...} <br>catch里面的语法也是match语法 |
| runtime exception | 需要catch | 不需要catch |
| 定长数组 | new String[3] | `new Array[String](3)` |
| 链表 | ArrayList| scala.collection.mutable.ArrayBuffer  功能等价于java的ArrayList，加数据可以用  buff += a|
| MAP | HashMap | scala.collection.mutable.HashMap 功能等价于java的HashMap|
| map.get() | String s = map.get("abc") | val s = map.getOrElse("abc",null)<br>scala里HashMap.get返回的是Option对象，一般不用 |
| 集合 | HashSet | scala.collection.mutable.HashSet 功能等价于java的HashSet |
| Tuple | 无 | Tuple2,Tuple3,... 对象，非常好用 |
| 多重赋值 | 不支持 | val (ret1,ret2,...) = xxx, 可使用此方法从Tuple中直接取值, <br>scalabpe的flow使用这个从并行调用中获取结果  |
| 列表LIST | 无 | scala里的List和java的List没任何关系，也完全不是一回事; <br>主要是用来做函数式编程的, 在scalabpe里请勿使用! |
| 对象根 | Object | Any, 一切都是对象; Object是Any的一个子类 |
| 类的静态成员 | static | class 内都是实例成员, 静态成员放在一个同名的object单例对象内; 也可单独定义没有class的object对象  |
| 接口申明 | interface | trait 可有具体实现代码 |
| 抽象类 | abstract class | trait |
| 类继承 | 只能继承(extends)一个类，实现(implements)多个接口 | 只能继承(extends)一个类, <br>但可混入(with)多个trait, <br>此特性非常好用!! |
| 函数也是对象 | 不支持 | 支持,scalabpe里invoke的callback就是函数;  |
| 类申明 | class  Address { ... } | 可同时申明实例成员, 并可带默认值, 如 class Address (val province:String, val:city:String, val: street:String = "") |

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

        name 对象的名称，区分大小写, 流程中使用此名称
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
        struct          scalabpe.core.HashMapStringAny 继承自 scala.collection.mutable.HashMap[String,Any]
        int array       scalabpe.core.ArrayBufferInt 继承自 scala.collection.mutable.ArrayBuffer[Int]
        string array    scalabpe.core.ArrayBufferString 继承自 scala.collection.mutable.ArrayBuffer[String]        
        struct array    scalabpe.core.ArrayBufferMap 继承自 scala.collection.mutable.ArrayBuffer[scalabpe.core.HashMapStringAny]

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

    <message name="testbatchupdate" id="7" isAck="true" retryInterval="30000" retryTimes="100">

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
    在接收到客户端的必达消息时，只要一完成消息的持久化，就会先回一个ACK消息给客户端，scalabpe自己会重试，不需要客户端再重试。

    为兼容老版本，如果发现avenue包头设置了"必达消息位"，即使服务描述文件中未申明也会作为必达消息处理

    必达消息的持久化数据保存在data/mustreach目录下，以服务名_消息名为队列名。

[返回](#toc)

# <a name="flow">流程文件格式</a>

## 目录结构

compose_conf 目录下可以有2种文件：scala文件和flow文件

__建议将scala后缀的文件直接放在compose_conf下__

__而flow文件则根据服务名分别放在对应服务名的子目录下__

__flow文件的命名建议用 消息名_消息号.flow 的格式__

## 文件类型

    .scala后缀的纯scala文件, 一般是一些辅助类，轻量级插件类等, 也可以直接写流程
    对.scala后缀的类文件，包名建议统一用scalabpe.flow，简化import语句

    .flow结尾的流程文件, 每个flow就是服务描述文件中的一个消息的实现
    每个flow文件在编译时会自动转换成scala类，编写flow文件，实际就是写scala类
    所以scala的语法在flow里可以没有限制的使用
    所有flow文件的包名会自动设置为scalabpe.flow, 并自动加上对scalabpe.core的import

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

      默认流程都继承类scalabpe.core.Flow, 使用特殊语法可以修改继承的基类

      使用//$xxx.xxx.with(baseclass)语法可以让流程继承一个用户自定义的基类baseclass,
      baseclass必须继承scalabpe.core.Flow, 用户可以在基类中做一些特殊处理，
      如在每个流程开始前自动从redis中加载http的session信息到内存中的session变量中，
      流程中只要操作session变量就能读写会话信息

[返回](#toc)

# <a name="compile">流程编译</a>

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

# <a name="writeflow">流程编写基础知识</a>

## 定义流程变量

    在//#receive入口函数的前面可以定义流程变量, 流程变量不需做多线程同步处理
    
    var a = ""  // 定义一个字符串
    var i = 0 // 定义一个int
    var r:InvokeReult = _ // 定义一个保存调用结果的变量

    事实上, 可以使用任何scala语法定义变量，流程内的公共函数，甚至嵌套类

## 获取入参

    流程中可直接访问req对象获取入参

    class Request (

        val requestId : String,
        val connId : String,
        val sequence : Int,
        val encoding : Int,

        val serviceId : Int,
        val msgId : Int,

        val xhead : HashMapStringAny,
        val body : HashMapStringAny,
        val sender: Actor
    )

    Request对象有如下方法可以使用来简化参数获取:

    .s(name)  从body中获取一个字符串, 会自动进行类型转换
    .s(name,defaultValue) 带默认值的 .s
    .ns(name)  返回非null的string
    .ns(name,defaultValue) 带默认值的 .ns

    .i(name)  从body中获取一个int, 会自动进行类型转换
    .i(name,defaultValue) 带默认值的 .i

    .l(name)  从body中获取一个long, 会自动进行类型转换
    .d(name)  从body中获取一个double, 会自动进行类型转换
    .bd(name)  从body中获取一个BigDecimal, 会自动进行类型转换

    .m(name)  从body中获取一个HashMapStringAny, 对应服务描述文件里的struct
    .nm(name)  返回非null的map

    .ls(name)  从body中获取一个字符串数组, 对应服务描述文件里的string array
    .nls(name)  返回非null的array

    .li(name)  从body中获取一个int数组, 对应服务描述文件里的int array
    .nli(name)  返回非null的array

    .lm(name)  从body中获取一个对象数组, 对应服务描述文件里的struct array
    .nlm(name)  返回非null的array

    .xs(name)  从xhead中获取一个字符串, 会自动进行类型转换
    .xs(name,defaultValue) 带默认值的 .xs
    .xi(name)  从xhead中获取一个int, 会自动进行类型转换
    .xls(name:String) : ArrayBufferString 用来取 gsInfos

    扩展包头xhead中的key支持以下值，参见 src/codec.AvenueCodec object，常用的有以下几个：

        gsInfoFirst  最远的调用者的IP:PORT
        gsInfoLast   最近的调用者的IP:PORT
        gsInfos"     所有的调用者，需用.xls来获取
        socId        客户端标识
        appId        应用ID

    .remoteIp 取对端IP
    .remoteAddr 取对端IP和端口
    .clientIp 取客户端IP

## 获取配置参数

    对config.xml里 <Parameter name="xxx">yyy</Parameter>

    可用Flow.router.getConfig("xxx","")获取到yyy

## 调用服务

    使用invoke系列方法来调用接口

    调用1个接口：
        invoke(callbackfunction,"servicename.msgname",timeout,动态参数列表)

    并行调用2个接口：    
        invoke2(callbackfunction,
             invokeFuture("servicename.msgname",timeout,动态参数列表),
             invokeFuture("servicename.msgname",timeout,动态参数列表)   )

    并行调用3,4,5个接口：    
        分别使用invoke3,invoke4,invoke5方法

    并行调用5个以上的接口：    
        invokeMulti(callbackfunction,ArrayBuffer[InvokeInfo]) 
        调用invokeFuture可生成InvokeInfo对象
        使用invokeMulti可动态生成并行调用的请求

## 动态参数列表

    动态参数列表的个数是没有限制的，可以有多种方式传递参数

    每个参数按名称指定：

        "appId"->req.i("appId"),"areaId"->2,"userId"->"123"

    如果多个参数appId,areaId都来源于Request对象，可以这样：

        "appId,areaId"->req,"userId"->"123"

    如果个别参数appId,areaId调用新接口时名称不一样，可以这样：
    
        "appId:appId2,areaId:areaId2"->req,"userId"->"123"

        新接口里的入参名是appId2和areaId2

    如果所有参数appId,areaId,userId都来源于Request对象，可以这样：

        "*"->req

    如果所有参数appId,areaId,userId都来源于Request对象，但个别值，比如userId需要调整，可以这样：

        "*"->req,"userId"->"123"    后指定的值会覆盖前面指定的值

    类似上面从Request取参数，还可以从HashMapStringAny或InvokeResult对象取值

    如:
        val m = HashMapStringAny("appId"->,"areaId"->2,"userId"->"123")
        invoke(callbackfunction,"a.b",3000,"*" -> m)

    和

        val ret = lastresult()
        invoke(callbackfunction,"a.b",3000,"*" -> ret)

## 指定扩展包头里的参数

    扩展包头的参数会自动继承当前req对象里的扩展包头参数，也可以额外添加或修改参数值

    在invoke的时候，如果动态参数名是以head.开头的参数，则可设置扩展包头的参数，可能会有： 

    head.encoding 指定发出调用的字符集，utf-8或gbk, 默认为utf-8
    head.socId  客户端标识
    head.appId  应用标识

## 获取调用结果

    并行调用，只有所有接口都得到响应或超时后回调才会执行

    对于invoke:
        val ret = lastresult()

    对于invoke2:
        val (ret1,ret2) = lastresults2()

    对于invoke3:
        val (ret1,ret2,ret3) = lastresults3()

    对于invoke4:
        val (ret1,ret2,ret3,ret4) = lastresults4()

    对于invoke5:
        val (ret1,ret2,ret3,ret4,ret5) = lastresults5()

    对于invokeMulti:
        val rets = allresults() // 可用rets(i)取对应的结果，结果顺序和调用顺序一致
        invoke2,invoke3,invoke4,invoke5 也可使用 val rets = allresults() 来获取结果

    每个调用得到的结果是一个 InvokeResult 对象

    class InvokeResult(val requestId:String, val code:Int, val res:HashMapStringAny)

    InvokeResult 的code是错误码, res是调用的具体信息
    InvokeResult 同Request对象一样，具有 .s .i .m .ls .li .lm 方法简化从结果中获取对象

## 输出响应

    reply(code,动态参数列表)
    
    code为错误码，也就是avenue协议头的code位

    ScalaBPE约定，code=0表明成功 code<0表示错误

    动态参数列表 的使用同invoke调用时, 示例：

    val ret = lastresult()

    reply(0,"appId"->1,"areaId"->2,"userId"->"123")

    reply(0,"appId,areaId"->req,"userId"->"123")

    reply(0,"*"->req)

    reply(0,"*"->ret)

    reply(0,"*"->ret,"userId"->"123")

    传统的同步编程返回响应总是在代码的最后，返回后就不可以再执行任何代码了
    在ScalaBPE里，可以在流程中的任何地方调用reply, 调用完reply后可以继续执行代码

    ScalaBPE里每个请求最多只允许调用一次reply。

## 投递

    当你想调用一个方法，而并不关心其返回结果可以使用invokeWithNoReply方法

    invokeWithNoReply("servicename.msgname",动态参数列表)

    invokeWithNoReply方法不用指定回调函数和超时时间

## invokeAutoReply系列方法

    如果只想关注正常流程，对出错的情况(错误码非0的情况)希望直接结束流程，可以通过invokeAutoReply简化流程编写

    invokeAutoReply(...)
    invoke2AutoReply(...)
    invoke3AutoReply(...)
    invoke4AutoReply(...)
    invoke5AutoReply(...)
    invokeMulyiAutoReply(...)

    调用参数完全同invoke系列方法，处理逻辑：
        如果有任意一个invoke的错误码非0，则直接结束流程；
        默认的错误码为invokeResult里的code,可以通过在动态入参中传递一个errorCode来指定要返回的错误码
        对多个invoke并行调用，使用传入的最后一个errorCode动态入参

## 调用有状态的服务器

    如果一个服务器连接多个远程服务器，需要将消息按一定规则分发给指定服务器而不是随机分发

    这时可使用 ToAddr系列方法:

    invokeWithToAddr(callbackfunction,"servicename.msgname",timeout,toAddr,动态参数列表)
    invokeFutureWithToAddr("servicename.msgname",timeout,toAddr,动态参数列表)
    invokeWithNoReplyWithToAddr("servicename.msgname",toAddr,动态参数列表)

    toAddr为远程服务器的IP:PORT

    如何确定toAddr由自己实现逻辑确定

## 睡眠

    传统的睡眠会挂起当前线程，如果你希望流程睡眠一段时间但又不挂起当前线程，可以使用sleep函数

    sleep(callbackfunction,timeout)

    在timeout时间过后流程会继续

[返回](#toc)

# <a name="loop">流程中如何实现循环</a>

    传统的循环使用while,for等, 在ScalaBPE里，不涉及到invoke异步调用的地方可以用传统的循环。
    但是如果一个循环中包含invoke异步调用，就必须调整流程的编写模式

    将要循环的数据保存到流程变量loopData中, 通过loopIdx每次以异步的方式处理一条数据，直到
    loopIdx >= loopData.size

        var loopData: ArrayBufferMap = _ // 或其他数据结构 Array, ArrayBuffer等
        var loopIdx = -1 

    示例代码：

        loopData = ...  // 赋值
        doLoop  // 进入第一次循环

        //#doLoop

        loopIdx += 1
        if( loopIdx >= loopData.size) return // 结束整个流程

        val data = loopData(loopIdx) // 获取当前数据

        invoke(abc,"svr.msg",3000,"*"->data) // 以异步的方式处理data

        //#abc

        val ret = lastresult()

        // 处理ret

        doLoop // 重新跳到循环入口

[返回](#toc)

# <a name="json">Json转换</a>

    在流程中可以直接使用 JsonCodec 类来处理json数据
    JsonCodec常用方法：

        def parseSimpleJson(s:String):HashMapStringAny // 解析无嵌套的json串 
        def parseJson(s:String):Any // 解析嵌套json串，根据顶层类型可能是
                                    // HashMapStringAny或ArrayBufferAny，需自己转换类型
        def parseObject(s:String):HashMapStringAny // 解析嵌套对象
        def parseArray(s:String):ArrayBufferAny // 解析嵌套数组
        def parseArrayInt(s:String):ArrayBufferInt // 解析嵌套数组
        def parseArrayString(s:String):ArrayBufferString // 解析嵌套数组
        def parseArrayObject(s:String):ArrayBufferMap // 解析嵌套数组

        def parseObjectNotNull(s:String):HashMapStringAny // 解析嵌套对象
        def parseArrayNotNull(s:String):ArrayBufferAny // 解析嵌套数组
        def parseArrayIntNotNull(s:String):ArrayBufferInt // 解析嵌套数组
        def parseArrayStringNotNull(s:String):ArrayBufferString // 解析嵌套数组
        def parseArrayObjectNotNull(s:String):ArrayBufferMap // 解析嵌套数组

        def mkString(m:HashMapStringAny): String // map转换成json串
        def mkString(a:ArrayBufferInt): String // array转换成json串
        def mkString(a:ArrayBufferString): String // array转换成json串
        def mkString(a:ArrayBufferMap): String // array转换成json串
        def mkString(a:ArrayBufferAny): String // array转换成json串

[返回](#toc)

# <a name="localstorage">本地存储</a>

    在流程中可以直接使用 LocalStorage类来保存和加载数据到本地文件系统中
    LocalStorage常用方法：

    def save(key:String,value:String) 
    def save(key:String,o:HashMapStringAny) 
    def save(key:String,a:ArrayBufferString) 
    def save(key:String,a:ArrayBufferInt) 
    def save(key:String,a:ArrayBufferMap)
    def loadString(key:String):String  
    def loadMap(key:String):HashMapStringAny 
    def loadStringArray(key:String):ArrayBufferString 
    def loadIntArray(key:String):ArrayBufferInt 
    def loadMapArray(key:String):ArrayBufferMap 

    LocalStorage按key将数据保存在data/localstorage/key的文件中
    不建议用LocalStorage存储粒度太细的数据，否则文件太多，建议存储集合类的数据

[返回](#toc)

# <a name="global">自定义启动和关闭函数</a>

    可以在compose_conf下自定义一个类以便在框架启动和关闭时执行某些操作，

    用途：

        1) 在init中加载较复杂的配置数据，如复杂的xml数据，此时加载无需考虑多线程问题
        2) 和Quartz插件的Init配置配合使用，加载数据库配置保存在本地，在数据库无法连接时
           也能启动，用来替代localcache，加载较复杂的配置数据
        3) 使用框架编写数据汇总服务，在流程中对数据进行累加，每隔几分钟更新数据库，在框
           架关闭时保存到本地，启动时再恢复数据

    用法：

        package scalabpe.flow

        object Global {
            def init() {
                println("init called")
            }
            def close() {
                println("close called")
            }
        }

        要求类名： object scalabpe.flow.Global
        函数：init 启动时调用
        函数：close 关闭时调用

[返回](#toc)

# <a name="baseflow">实现自定义的基类</a>

    可以自定义流程基类，比如对session的处理

    示例：

    abstract class YourBaseFlow extends Flow {
        // 重载你需要的函数
    }

    在流程中//$servicename.msgname.with(YourBaseFlow)来使用你自定义的基类

    YourBaseFlow中可通过重载Flow基类中预定义的几个函数进行功能扩展：

    共有以下的可重载点:

    // 在流程开始执行前会调用此函数处理req.body, 重载函数内应只对map进行操作
    def filterRequest(map:HashMapStringAny) : Unit = {}  

    // 在receive执行前会调用此函数，此函数最后需调用receive()才能走到流程的标准入口, 此函数内可做任意异步调用并等待返回
    def baseReceive() : Unit = { receive() } 

    // 发出的任意一个调用都会调用此函数处理入参, 重载函数内应只对map进行操作
    def filterInvoke(targetServiceId:Int,targetMsgId:Int,map:HashMapStringAny) : Unit = {} 

    // 在真正的reply前会调用此函数处理响应结果, 重载函数内应只对map进行操作
    def filterReply(code:Int,map:HashMapStringAny) : Unit = {} 

    // 在流程结束前会调用此函数, 可发出不需要等待响应的请求
    def baseEndFlow(): Unit = {} 

[返回](#toc)

# <a name="avenue">Avenue协议</a>

## 介绍

  avenue协议是自定义的TCP长连接协议，ScalaBPE框架使用此协议和其它Avenue服务通讯
  
  了解底层通讯协议有助于更好地理解ScalaBPE框架

## 请求格式  version=1
    
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
    
    headLen 填44 + 扩展包头长度, 包头最多可以支持到256个字节
    
    version 填1, 
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

## 响应格式 version=1
        
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
    
    headLen 填44 + 扩展包头长度, 包头最多可以支持到256个字节
    
    version 填1
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

## 请求格式  version=2 从1.2.6版本开始支持version=2
    
    0........ 8........16........24........32
    1  |--0xA1---|headLen-|--version-|----M---| //协议标记,头长,版本号,路由数
    2  |---------------packetLen--------------| //包长 
    3  |---------------serviceId--------------| //服务id
    4  |----------------msgId-----------------| //请求id
    5  |---------------sequence---------------| //序列号
    6  |---------------optional---------------| //可选标记位
    7  |---------------priority---------------| //优先级
    
    headLen ( 填28 + 扩展包头长度按4字节对齐 ) / 4 (相比version=1,实际包头长度需*4, 包头最多可以支持到1024个字节)
    version 填2
    去掉version=1里的签名
    其它属性同version=1

## 响应格式 version=2 从1.2.6版本开始支持version=2
        
    使用标准的Avenue协议包头, 无扩展包头: 

    0........ 8........16........24........32
    1  |--0xA2---|headLen-|--version-|reserve-| //协议标记，头长，版本号，保留位
    2  |---------------packetLen--------------| //包长 
    3  |---------------serviceId--------------| //服务id
    4  |----------------msgId-----------------| //请求id
    5  |---------------sequence---------------| //序列号
    6  |---------------optional---------------| //可选标记位
    7  |-----------------code-----------------| //响应码
    
    其中：
    
    headLen ( 填28 + 扩展包头长度按4字节对齐 ) / 4 (相比version=1,实际包头长度需*4, 包头最多可以支持到1024个字节)
    version 填2
    去掉version=1里的签名
    其它属性同version=1

[返回](#toc)

# <a name="tlv">TLV编码规范</a>

## 介绍

  tlv编码用于avenue协议的包体body和扩展包头xhead的数据编码。

## tlv编码基本知识

    TLV是一种可变格式，意思是：

| T | L | V | 
| --- | --- | --- |
| Type | Length | Value |
| 类型 | 长度 | 值 |
| 2字节 | 2字节 | 长度为Length-4 |

    Type和Length的长度固定，一般是2或4个字节
     
    Avenue协议中约定Type和Length使用2字节, Length包括Type和Length自身的字节数, 所以Length最少为4

    Avenue协议中约定所有Short,Int类型都采用网络字节序，包括Type,Length,Int类型的值，SystemString长度前缀等

    Value的长度由Length指定, Avenue协议中Value的类型有以下几种：

        1) Int类型， 这时Length固定为8

        2) Long类型， 这时Length固定为16, 从1.2.6版本开始支持

        3) Double类型， 这时Length固定为16, 从1.2.6版本开始支持

        4) String类型，这时Length为String的实际长度+4, 需对齐, Length不包括填充的字节数

        5) Struct类型，这时Length为Struct的实际长度+4, Length不包括填充的字节数

            Struct类型由多个Field组成，Field的类型有3种：

                Int类型，其长度固定为4
                Long类型，其长度固定为8
                Double类型，其长度固定为8
                String类型，用Len属性指定实际长度，需对齐, Len不包括填充的字节数, Len建议用4的倍数
                SystemString类型，可变长度String, 每个SystemString由4字节的长度前缀和实际内容组成, 需对齐
                VString类型，可变长度String, 每个VString由2字节(特别长6字节)的长度前缀和实际内容组成, 需对齐

                多个Field按顺序连续编码

        6) Object类型，这时Length为Object的实际长度+4, Length不包括填充的字节数, 从1.2.6版本开始支持

            Object类型由多个Field组成，每个Field引用一个已存在的类型，可以嵌套

        7) Array类型, 包括Int Array, Long Array, Double Array, String Array, Struct Array, Object Array

            Avenue协议只需按顺序对数组中每个元素单独编码即可

    对齐和填充:

        在Avenue协议中，String/SystemString/VString类型的数据都需要对齐到4字节，不足4字节用'\0'填充

    T0LV扩展编码方案:

        上述TLV编码方案中Length最大只能为65535, 超过此长度的需用向下兼容的T0LV扩展编码方案

| T | 0 | L | V | 
| --- | --- |--- | --- |
| Type | 固定填0 | Length | Value |
| 类型 | 固定填0 | 长度 | 值 |        
| 2字节 | 2字节 | 4字节 | 长度为Length-8 |

## 扩展包头的TLV编码

    通过服务描述文件，用户可以自己定义TLV, 但是无法自定义扩展包头的TLV;
    扩展包头的Type是Avenue协议约定好的，每个Type都有特殊含义;
    扩展包头里存放的信息一般较少，不支持 T0LV 扩展编码方案;

    扩展包头定义参见 src/codec.AvenueCodec object，包括：

| Key | Type | 含义 | 类型 | Length | 
| --- | --- | --- | --- | --- |
| socId | 1 | 客户端标识 | string | 可变 |
| gsInfos | 2 | 所有调用者的IP:PORT | struct array { ip int, port int} | 每个struct 12字节 |
| appId | 3 | 应用ID | int | 8 |
| areaId | 4 | 区ID | int | 8 |
| groupId | 5 | 组ID | int | 8 |
| hostId | 6 | 主机ID | int | 8 |
| spId | 7 | SP ID | int | 8 |
| endpointId | 8 | 终端ID | string | 可变 |
| uniqueId | 9 | 请求ID | string | 可变 |
| spsId | 11 | SPS服务ID | string | 可变 |
| httpType | 12 | 是否HTTPS | int | 8 |

[返回](#toc)
