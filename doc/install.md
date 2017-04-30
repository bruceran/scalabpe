# <a name="toc">目录</a>

[安装JDK和SCALA](#install)

[ScalaBPE框架源码编译环境配置](#source)

[基于ScalaBPE框架的业务项目配置](#project)

[框架依赖的jar包说明](#jars)

# <a name="install">安装JDK和SCALA</a>

## 支持的操作系统

	此框架在linux和windows下运行良好，未在其它操作系统下测试过
	此框架基于JDK, JDK能正常运行的操作系统理论上应该都能正常运行此框架，但可能编译和运行脚本需要调整

## 安装JDK

	* 安装

	安装Sun JDK1.8或以上版本, 使用64位jdk版本

	* 设置好JAVA_HOME和PATH环境变量

	JAVA_HOME 配置为JDK安装目录
	PATH中增加JDK安装目录下的bin目录为查找路径

	linux下示例：
	编辑.bash_profile
	    export JAVA_HOME=/usr/local/jdk1.8.0_101
	    export PATH=$JAVA_HOME/bin:$PATH

	在命令行下运行java -version, 若配置正确会显示版本信息

## 安装scala

	scalabpe框架的lib目录下已包括scala 2.12.1的运行所需的jar包，不安装scala就可以运行scalabpe框架
    只在开发人员需要独立启动scala程序做学习或其它开发才需要单独安装scala

	* 下载 

	官网下载地址: http://www.scala-lang.org/download/all.html

	建议下载 scala-2.12.x(x>=1)的版本, 不能使用低于此版本的scala

	* 安装

	scala安装包为一个zip包，直接解压到目标目录后就可使用

	* 设置SCALA_HOME和PATH环境变量

	SCALA_HOME 配置为scala安装目录
	PATH中增加scala安装目录下的bin目录为查找路径

	linux下示例：
	编辑.bash_profile
	    export SCALA_HOME=/usr/local/scala-2.12.1
	    export PATH=$SCALA_HOME/bin:$PATH

	为脚本增加可执行权限: 
	cd /usr/local/scala-2.12.1/bin && chmod 755 *

	在命令行下运行scala -version, 若配置正确会显示版本信息

[返回](#toc)

# <a name="source">ScalaBPE框架源码编译环境配置</a>

## 目录结构说明

	avenue_conf/ 服务描述文件目录, 支持多级子目录
	compose_conf/ 流程文件目录, 支持多级子目录
	data/  数据文件目录
	doc/ 文档目录
	lib/ 依赖的jar包
	src/ 框架核心和插件代码目录，scalabpe-core-1.2.x.jar 和 scalabpe-plugins-1.2.x.jar 的源码 
	temp/ 运行时临时目录
	testcase/ 接口测试指令文件, 用于开发人员或测试人员测试接口
	third_party/ 第三方插件目录
	webapp/  httpserver服务使用的相关目录
	webapp/static/  静态文件目录, web根目录
	webapp/template/  模板文件目录
	webapp/upload/  上传文件目录

	build   linux下编译脚本
	build.bat  windows下编译脚本
	config.xml 应用配置文件
	LICENSE  许可证
	logback.xml  日志配置文件
	README.md 本文件
	service   linux下运行脚本
	service.bat  windows下运行脚本

## 启动/停止服务

	* 在linux下：

	service start 启动服务
	service stop 停止服务
	service restart 重启服务
	service status 查看状态
	service runtest 发送接口测试指令
	service help 帮助

	* 在windows下：

	service 启动服务, 按Ctrl+C停止服务
	service runtest 发送接口测试指令

	windows下的service脚本是个简化脚本，只适用于开发人员使用，不适合生产环境运行

## 编译src/目录下的源码

	运行 build 脚本进行编译, build脚本只用于编译src/目录下的源代码, compose_conf下的在每次启动时编译
	编译后会生成2个jar包：lib/scalabpe-core-1.2.x.jar 和 lib/scalabpe-plugins-1.2.x.jar 
	每次修改src下的代码后需要重新编译再运行service脚本才能看到最新效果

[返回](#toc)

# <a name="project">基于ScalaBPE框架的业务项目配置</a>

## 目录结构

	avenue_conf/ 
	compose_conf/
	lib/
	webapp/    对外提供http服务才需要此目录，否则可删除
	config.xml
	logback.xml
	service
	service.bat
	data/,temp/ 目录会在第一次启动时自动创建

## 说明
		
	* 只是源码编译环境的一个精简版, 使用相同的service脚本来启动停止服务
	* 去除了所有运行时不需要的目录和文件, 只保留必需的目录
	* 没有build编译脚本，compose_conf下的流程文件会在启动时自动进行编译

[返回](#toc)

# <a name="jars">框架依赖的jar包说明</a>

## 必须包含的jar包

	* scala 2.12.1 版本运行jar包

    scala-compiler.jar
    scala-library.jar
    scala-reflect.jar
    scala-xml_2.12-1.0.6.jar

	* scalabpe核心包和插件包

	scalabpe-core-1.2.x.jar
	scalabpe-plugins-1.2.x.jar

	* 基于文件的队列实现

	billing-queue-1.1.2.jar  源码地址：https://github.com/bruceran/billing-queue

	* 核心依赖包

	slf4j-api-1.6.6.jar
	logback-core-1.0.13.jar
	logback-classic-1.0.13.jar
    jcl-over-slf4j-1.6.6.jar
    log4j-over-slf4j-1.6.6.jar
	commons-logging-1.1.1.jar
	commons-logging-api-1.1.jar
	commons-lang3-3.1.jar
	commons-codec-1.6.jar
	commons-io-1.4.jar
	commons-lang-2.6.jar
	commons-pool-1.6.jar, dbcp和jedis使用
	commons-dbcp-1.4.jar
	commons-collections-3.2.1.jar

	* 网络层组件

	netty-3.2.6.Final.jar

	* json组件

	jackson-core-2.2.3.jar
	jackson-databind-2.2.3.jar
	jackson-annotations-2.2.3.jar

## 可选的jar包

	* quartz插件

	quartz-2.2.1.jar

	* 数据库插件

	mysql-connector-java-5.1.18.jar
	ojdbc14-10.2.0.5.0.jar
	sqljdbc4.jar

	* redis插件

	jedis-2.1.0.jar

	* xmemcached插件
	
	xmemcached-2.0.0.jar

	* httpserver插件中用到的velocity模板引擎

	oro-2.0.8.jar
	velocity-1.7.jar

	* mail插件

	activation.jar
	mail-1.4.5.jar

[返回](#toc)
