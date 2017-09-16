
# 从1.1.x升级到1.2.x(master分支)说明

    需要将jdk从1.6换成1.8
    需要将scala从2.10.2换成2.12.1
    1.2.x的scalabpe的lib/目录下已经包括了scala 2.12.1的运行依赖的jar文件，在服务器上只需安装jdk1.8就可以运行scalabpe 1.2.x
    1.2.x的scalabpe的顶层包名从jvmdbbroker改成了scalabpe,不再完全向下兼容
    老的1.1.x版本要升级1.2.x需要做以下事情：

        a) 升级jdk到1.8
        b) 将lib/下的scalabpe包替换成1.2.x包并增加scala语言包：
                scala-compiler.jar
                scala-library.jar
                scala-reflect.jar
                scala-xml_2.12-1.0.6.jar
        c) 替换原来lib/linux-jdk64/和lib/linux-jdk32/下的libsec.so文件(libsec.so需对应的源码需修改成新的包名scalabpe然后编译得到libsec.so)
        d) 替换原来的service service.bat脚本
        e) 将原来compose_conf下所有.scala后缀文件里的jvmdbbroker换成scalabpe
        f) 将原来的logback.xml里的jvmdbbroker替换成scalabpe
        h) 删除原来的temp下的所有文件以免不必要的错误
        i) 服务描述文件enableExtendTlv默认值从false改为true, 对超过64k的内容默认会采用t0lv方案，如果不希望可在服务描述文件里加 enableExtendTlv="false" 关闭
        j) parameter xml中值如果包含&这样的特殊字符，新版本的处理不向下兼容，有特殊字符，如 & <> 需要都修改成使用CDATA方式 <assign><![CDATA[@xxx=yyyy]]></assign>

# 核心

[release notes for scalabpe-core](../src/scalabpe/core/release_notes.txt)

此release notes文件会打包在每个 scalabpe-core jar文件中, 通过查看jar包内的release notes就可清楚了解当前版本信息

# 插件

[release notes for scalabpe-plugins](../src/scalabpe/plugin/release_notes.txt)

此release notes文件会打包在每个 scalabpe-core jar文件中, 通过查看jar包内的release notes就可清楚了解当前版本信息

