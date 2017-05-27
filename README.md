# ScalaBPE

* ScalaBPE 的含义是可伸缩的业务流程引擎(Scalable Business Process Engine)
* 此框架用于服务端系统开发
* 此框架使用scala语言开发, 2013年11月发布第一个版本
* 特别提醒：master分支已改为1.2.x版本，1.1.x版本改为v1.1.x分支, v1.2.x开始要求使用jdk1.8+scala2.12.1版本编译, 包名从jvmdbbroker修改成了scalabpe

# 用户手册

## [Release Notes](doc/releasenotes.md) 

    从1.1.x升级到1.2.x说明
	版本变更说明

## [安装指南](doc/install.md) 

	安装JDK
	安装Scala
	框架目录结构
	框架依赖的jar包说明

## [开发指南](doc/develop.md)

	Scala/Java语法对比
	服务描述文件
	流程文件格式
	流程编译
	流程编写基础知识
	流程中如何实现循环
	Json转换
    本地存储
    自定义启动和关闭函数
	Avenue协议
	TLV编码规范

## [辅助开发工具](doc/tools.md)

	GenSdfTool: 服务描述文件xml/txt转换，格式化，修补工具
	GenSqlTool: 生成db服务描述文件
	GenFlowTool: 根据服务描述文件生成流程文件和url mapping
	RenameFlowTool: 对服务描述文件按指定格式进行批量改名

## [配置指南](doc/config.md) 

	了解confg.xml的所有细节
	数据库连接密码加密
	参数文件

## [测试指南](doc/test.md) 

	如何使用测试工具, 编写单元测试和集成测试用例
	日志目录说明
	日志格式说明
	如何调整日志输出
	框架用到的错误码

## [插件开发指南](doc/plugin.md) 

	轻量级插件开发
	重量级插件开发
	HBASE插件开发示例

## [其它](doc/other.md) 

	TIPS

