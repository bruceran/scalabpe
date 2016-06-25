
# TIPS

* 熟读源码中的src/core.scala文件，里面包括了流程编写要用到的核心数据类型

* 当服务描述文件或流程文件在一个目录中太多时，可以建立子目录分类存放

* 流程中的println, log.debug, log.info 日志应在上线前注释掉，同步打日志非常耗时; 
  应熟练使用request_audit.log csos_audit.log 这2个异步日志排查问题

* invoke服务调用后都应检查返回码，所有的异常都是以返回码形式的，而不是抛异常；
  注意：访问db服务也可能超时; 如不需要检查返回码的就应该直接用invokeWithNoReply

* 使用invokeWithNoReply进行消息投递

* 流程中的callback function除了可以作为invoke的回调函数调用，也可以直接调用

* 使用*传递多个参数 invoke(..., "*" -> req, "appId" -> 233 )  

        假设req中已经包括appId 101, 则后设置的233会覆盖前面*传入的值
        超过5个以上的参数建议都用*号传递参数; jvmdbbroker在转发消息前
        会根据服务描述文件进行多余数据的过滤和类型转换;

* auto reply

        如果流程结束而没有使用reply, 那流程引擎会自动用最后一次invoke的调用结果作为返回码;
        如最后一次调用是并行调用，取最后一个错误码返回；

