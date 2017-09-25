# <a name="toc">目录</a>

[轻量级插件](#light)

[重量级插件](#heavy)

[Hbase插件开发示例](#hbase)

# <a name="light">轻量级插件</a>

	轻量级插件指的是框架或插件暴露出的一些接口，开发人员只要写一个类实现特定的接口就可完成某些
	框架或插件自身无法支持的功能，如db的分表算法，http client请求参数不是常规的form形式，而是
	xml形式, http server插件的输出是一个xml格式，而不是标准的json格式

	轻量级插件只要将实现类放入compose_conf目录就可以使用，框架启动时会自动编译该插件类
	轻量级插件的包名建议用 scalabpe.flow

	常用的插件：

## http client插件

    参考：src/scalabpe/plugin/http/httpclientplugin.scala

		trait HttpPlugin {}

        // adjust request body
        trait HttpPreRequestPlugin {
            def adjustRequestBody(msg: HttpMsgDefine, body: HashMapStringAny): Unit
        }

		// generate request string from body, including signature
		trait HttpRequestPlugin {
		  def generateRequestBody(msg:HttpMsgDefine,body:HashMapStringAny):String
		}

		trait RawHttpRequestPlugin {
		  def generateRequestBody(msg:HttpMsgDefine,body:HashMapStringAny):Array[Byte]
		}

		// generate signature and put signature and other necessary fields into body
		trait HttpSignPlugin {
		  def sign(msg:HttpMsgDefine,signatureKey:String,body:HashMapStringString):Unit
		}

		// parse contentStr to body, return resultCode
		trait HttpResponsePlugin {
		  def parseContent(msg:HttpMsgDefine, contentStr:String, body: HashMapStringAny ): Int
		}

    插件类必须继承 HttpPlugin 类 并实现 HttpRequestPlugin RawHttpRequestPlugin 
    HttpSignPlugin HttpResponsePlugin 中一个或多个
    插件类必须提供无参数的构造函数

    HttpPlugin 标记接口

    HttpPreRequestPlugin, 在解析要调用的url之前调用,以便做一些特殊处理

    HttpRequestPlugin 请求转换接口, 对文本类的请求，用户可自定义转换方法，
                      入参是body,返回值是http请求的实际内容

    RawHttpRequestPlugin 如果http请求是二进制内容，比如图片，需实现此方法，
                         入参是body,返回值是http请求的实际二进制内容

    HttpSignPlugin 签名插件，入参来自body, 签名后的信息也写入body

    HttpResponsePlugin 响应内容转化接口，如果响应不是json或简单的xml，需实现此接口来完成
                       contentStr到body的转换，返回值就是错误码

	示例：compose_conf/WeiXinPayHttpPlugin.scala
		
## http server 插件

    参考：src/scalabpe/plugin/http/httpserverplugin.scala

		trait HttpServerPlugin {}

		trait HttpServerRequestParsePlugin {
		  def parseContent(serviceId:Int,msgId:Int,charset:String,contentType:String,contentStr:String,body:HashMapStringAny ):Unit
		}

		trait HttpServerRequestPostParsePlugin {
		  def afterParse(serviceId:Int,msgId:Int,xhead:HashMapStringAny, body:HashMapStringAny ):Unit
		}

		trait HttpServerPreOutputPlugin {
		  def adjustBody(serviceId:Int,msgId:Int,errorCode:Int,body:HashMapStringAny):HashMapStringAny
		}

		trait HttpServerOutputPlugin {
		  def generateContent(serviceId:Int,msgId:Int,errorCode:Int,errorMessage:String,body:HashMapStringAny,pluginParam:String):String
		}

        trait HttpServerRawOutputPlugin {
            def generateRawContent(serviceId:Int,msgId:Int,errorCode:Int,errorMessage:String,body:HashMapStringAny,pluginParam:String,headers:HashMap[String,String]):Array[Byte]
        }

		trait HttpServerVerifyPlugin {
		  def verify(serviceId:Int,msgId:Int,xhead:HashMapStringAny,body:HashMapStringAny,httpReq:HttpRequest):Boolean
		}

    HttpServerPlugin 标记接口

    HttpServerRequestParsePlugin 请求参数解析插件
    HttpServerRequestPostParsePlugin  请求参数解析后处理插件, 在body已解析完毕后二次处理
    HttpServerPreOutputPlugin 输出预处理插件，可在正式输出前对body进行处理
    HttpServerOutputPlugin 输出插件
    HttpServerRawOutputPlugin 输出二进制内容插件, 此插件优先级高于HttpServerOutputPlugin
    HttpServerVerifyPlugin 签名校验插件, 该接口为同步接口，实现该接口时不能发生阻塞，否则影响性能

## db 插件

    参考：src/scalabpe/plugin/dbplugin.scala

    	// 分表插件
		trait SplitTablePlugin {
		  def generateTableIdx(req:Request):String
		}

		// 分库插件
		trait SplitDbPlugin {
		  def generateDbIdx(req:Request):Int
		}

[返回](#toc)

# <a name="heavy">重量级插件</a>

	重量级插件指的是真正的插件，比如去连接hbase读写数据的插件, 去访问mongo db的插件等,
	编写这类插件你需要深入了解框架的插件编写机制。

	ScalaBPE框架自带的插件(scalabpe-plugins-xxx.jar)和用户自己开发的插件地位是一致的，
    用户完全可以自己编写一个db插件替代框架自带的db插件。

## 插件类型：

	inithook		启动时加载配置参数的插件, 如你可以将某些配置参数放在一个集中的仓库里，
					参考 src/plugin/InitHookSample.scala
					此插件需实现  InitHook:

					trait InitHook {
					  def loadParameter(pmap:HashMapStringString): Unit;
					}

	request filter  对请求参数进行转换处理的插件, 
					参考 src/plugin/filter_sample.scala
					此插件需实现  RequestFilter:

					trait RequestFilter {
					  def filter(req: Request): Unit;
					}

	response filter 对响应参数进行转换处理的插件, 如错误码处理插件ErrorCodeCfg就是此类型,
					参考 src/plugin/filter_sample.scala
					此插件需实现  ResponseFilter:

					trait ResponseFilter {
					  def filter(res: Response, req: Request): Unit;
					}

	bean            只发出请求不接收请求的插件，如定时任务配置插件QuartzCfg
					参考 src/plugin/bean_sample.scala

					此插件需实现  Bean:

					trait Bean {}

	actor 			既可以接收请求也可以主动发出请求的插件，大部分都是此类插件, 
					如db插件,缓存插件,http插件等
					此类插件的开发可参考现有的db,缓存等插件

					此插件需实现  Actor:

					trait Actor {
					  def receive(v:Any): Unit;
					}

## 插件对象的定义

	class XxxPlugin(val router:Router,val cfgNode: Node)

	每个插件对象的构造函数都一样：
		router  框架的router对象
		cfgNode 该插件对应的config.xml文件里的配置部分

## 插件的打包

	每个插件编译后都需生成jar包，
	每个jar包根目录下需包含一个 scalabpe.plugins.conf文件，该文件用来申明插件及其配置
	每个生成好的jar包放入lib/目录, 调整config.xml 即可使用
	如插件有依赖的jar包，也需要一起放入lib/目录。

## scalabpe.plugins.conf格式说明

	#开头的配置行忽略
	插件类型,类名,config.xml对应的配置节点名
	示例：

		bean,scalabpe.plugin.SampleBean,SampleBeanCfg
		requestfilter,scalabpe.plugin.SampleRequestFilter,SampleRequestFilterCfg
		responsefilter,scalabpe.plugin.ErrorDescResponseFilter,ErrorCodeCfg
		actor,scalabpe.plugin.MailActor,MailCfg
		inithook,scalabpe.plugin.InitHookSample,InitHook

## 一些特殊trait

	实现插件的过程中可继承一些特殊trait，这些trait口会在适当时间点被框架调用

	Logging 继承此trait就可以直接使用一个log对象
	SyncedActor 该Actor插件若为同步插件，直接使用调用线程，没有自己的线程池，需继承此trait
	RawRequestActor 该插件可直接处理未做tlv解码的消息, 也就是RawRequest对象，需继承此trait
	Closable 该插件若有资源需要释放，需实现此trait
	AfterInit 某些插件希望分阶段启动, 框架会在所有actor都创建后再调用此方法
	BeforeClose 某些插件希望分阶段关闭, 框架会在实际关闭actor前先调用此方法
	InitBean 实现此trait表明此trait是一个特殊的bean, 框架会等待此bean的isInited返回true才会继续
	SelfCheckLike 该插件如果希望能被管理端口的 SelfCheck.do 触发，需实现此trait
	Refreshable 该插件如果希望能被管理端口的 NotifyChanged.do 触发，需实现此trait
	Dumpable 该插件如果希望能被管理端口的 Dump.do 触发，需实现此trait

[返回](#toc)

# <a name="hbase">Hbase插件开发示例</a>

	Hbase插件为一重量级插件， 在third_party/hadoop目录下

	文件说明：

		avenue_conf/  该插件支持的服务描述文件示例
		src/	 源码，release notes， 插件配置文件
		lib/ 编译和运行该插件需要的jar文件
		build linux下编译脚本 
		build.bat windows下编译脚本
		config.xml 该插件的配置脚本示例

	编译完成后做如下操作就可让框架启动时来加载该插件:

		将lib/hadoop下所有文件复制到项目的lib/hadoop目录下
		参照avenue_conf/hbase_980编写新的服务描述文件
		参照config.xml修改项目的config.xml

[返回](#toc)
