# 安装JDK和SCALA

## 支持的操作系统

此框架在linux和windows下运行良好，未在其它操作系统下测试过
此框架基于JDK, JDK能正常运行的操作系统理论上应该都能正常运行此框架，但可能编译和运行脚本需要调整

## 安装JDK

* 安装Sun JDK1.6或以上版本, 根据机器是32位还是64位选择正确的jdk版本

* 设置好JAVA_HOME和PATH环境变量:

		JAVA_HOME 配置为JDK安装目录
		PATH中增加JDK安装目录下的bin目录为查找路径

		linux下示例：
	    编辑.bash_profile
		    export JAVA_HOME=/usr/local/jdk1.6.0_43
		    export PATH=$JAVA_HOME/bin:$PATH

* 在命令行下运行java -version, 若配置正确会显示版本信息
* 此框架未在JDK 7或以上版本下编译测试过, 如有需要可自行解决兼容性问题

## 安装scala

* scala 2.10.2

		scala安装包scala-2.10.2.zip为一个zip包，直接解压后就可使用

* 设置好SCALA_HOME和PATH环境变量:

		SCALA_HOME 配置为scala安装目录
		PATH中增加scala安装目录下的bin目录为查找路径

		linux下示例：
	    编辑.bash_profile
		    export SCALA_HOME=/usr/local/scala-2.10.2
		    export PATH=$SCALA_HOME/bin:$PATH

	    cd /usr/local/scala-2.10.2/bin && chmod 755 *

* 在命令行下运行scala -version, 若配置正确会显示版本信息
* 此框架未在scala 2.11.x或以上版本下编译测试过, 如有需要可自行解决兼容性问题

# ScalaBPE框架源码编译环境配置

## 目录结构说明

	avenue_conf/ 服务描述文件目录, 支持多级子目录
	compose_conf/ 流程文件目录, 支持多级子目录
	data/  数据文件目录
	doc/ 文档目录
	lib/ 依赖的jar包
	src/ 框架核心和插件代码目录，scalabpe-core-1.1.x.jar 和 scalabpe-plugins-1.1.x.jar 的源码 
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

### 在linux下：

	service start 启动服务
	service stop 停止服务
	service runtest 发送接口测试指令
	service help 帮助

### 在windows下：

	service 启动服务, 按Ctrl+C停止服务
	service runtest 发送接口测试指令

## 编译src/目录下的源码

	运行 build 脚本进行编译
	编译后会生成2个jar包：lib/scalabpe-core-1.1.x.jar 和 lib/scalabpe-plugins-1.1.jar 
	每次修改src下的代码后需要重新编译再运行service脚本才能看到最新效果

# 基于ScalaBPE框架的业务项目配置

## 目录结构

	avenue_conf/ 
	compose_conf/
	lib/
	webapp/    对外提供http服务才需要此目录，否则可删除
	config.xml
	LICENSE
	logback.xml
	service
	service.bat
	data/,temp/ 目录会在第一次启动时自动创建

## 说明
		
* 只是源码编译环境的一个精简版, 使用相同的service脚本来启动停止服务
* 去除了所有运行时不需要的目录和文件, 只保留必需的目录
* 没有build编译脚本，compose_conf下的流程文件会在启动时自动进行编译

