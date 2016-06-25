
====  以下包必须包含 =============================

#scalabpe核心包和插件包
scalabpe-core-1.1.x.jar
scalabpe-plugins-1.1.x.jar

# 基于文件的本地队列
billing-queue-1.1.1.jar

#核心依赖包
slf4j-api-1.6.6.jar
logback-core-1.0.13.jar
logback-classic-1.0.13.jar
commons-logging-1.1.1.jar
commons-logging-api-1.1.jar
commons-lang3-3.1.jar
commons-codec-1.6.jar
commons-io-1.4.jar
commons-lang-2.6.jar
commons-pool-1.6.jar, dbcp和jedis使用
commons-dbcp-1.4.jar
commons-collections-3.2.1.jar

#网络层组件
netty-3.2.6.Final.jar

#Json组件
jackson-core-2.2.3.jar
jackson-databind-2.2.3.jar
jackson-annotations-2.2.3.jar

====  以下包根据使用的插件选用 ===================

#quartz插件使用
quartz-2.2.1.jar

#数据库插件使用
mysql-connector-java-5.1.18.jar
ojdbc14-10.2.0.5.0.jar
sqljdbc4.jar

#redis插件使用
jedis-2.1.0.jar

#xmemcached插件使用
xmemcached-2.0.0.jar

#httpserver插件使用， velocity模板引擎
oro-2.0.8.jar
velocity-1.7.jar

#mail插件使用
activation.jar
mail-1.4.5.jar

#mq插件使用
activemq-all-5.9.0.jar  从包中删除了logback和log4j，否则和框架使用的版本有冲突

