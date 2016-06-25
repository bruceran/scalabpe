# 轻量级插件

	
    2) 对db插件,aht插件都只需要提供一个实现特定接口的实现类，可直接放在流程目录下
        db插件涉及到的接口原型参考：src/jvmdbbroer/plugin/dbplugin.scala
        aht插件涉及到的接口原型参考: src/jvmdbbroer/plugin/http/httpclientplugin.scala

# 重量级插件

    1) 特定项目的一些特殊插件放在jvmdbbroker源码目录的third_party目录下，每个工程一个子目录，编译后为一个jar包
        只有涉及到写 actor,bean,request filter, response filter的插件才需要放在third_party目录下

