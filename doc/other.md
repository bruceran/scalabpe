
# TIPS

* 需要调整编程时的固定思维，由传统的同步调用编程模式改为异步回调式编程模式

* 熟读源码中的src/core.scala文件，里面包括了流程编写要用到的核心数据类型

* 当服务描述文件或流程文件在一个目录中太多时，可以建立子目录分类存放

* 流程中的println, log.debug, log.info 日志应在上线前注释掉，同步打日志非常耗时; 
  应熟练使用request_audit.log csos_audit.log 这2个异步日志排查问题

* invoke服务调用后都应检查返回码，所有的异常都是以返回码形式的，而不是抛异常；
  
* 使用invokeWithNoReply进行消息投递

* 使用invokeAutoReply只处理正常的消息，失败的消息由框架自动处理

* 传统上调用jdbc会阻塞，jdbc阻塞多久应用程序线程就等多久，ScalaBPE里调用DB服务可设定超时时间; 

* 每个函数在发出invoke后不应该再继续执行任何代码，流程引擎同时只允许一个invoke；
  在if条件分支里的invoke后面别忘了return结束该函数, 否则流程会继续往下执行，很有可能造成同时多个invoke

* 流程中的callback function除了可以作为invoke的回调函数调用，也可以直接调用

* auto reply

        如果流程结束而没有使用reply, 那流程引擎会自动用最后一次invoke的调用结果作为返回码;
        如最后一次调用是并行调用，取最后一个错误码返回；

