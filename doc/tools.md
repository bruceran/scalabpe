# <a name="toc">目录</a>

[辅助开发工具](#tools)

# <a name="tools">辅助开发工具</a>

## 工具包中的辅助工具列表

    要使用此工具包，需要将将scalabpe-tools-0.1.x.jar包拷贝到你自己的lib目录下
    scalabpe-tools的源码在third_party/tools目录下

	输入>scala -cp "lib\*;." scalabpe.Tools 查看tools jar包支持的所有辅助工具
    输入>scala -cp "lib\*;." scalabpe.XxxTool 可查看每个命令的使用帮助

    scalabpe.GenSdfTool        在服务描述文件和等价的文本文件之间进行转换，自动补全type等操作
    scalabpe.GenSqlTool        生成db服务描述文件
    scalabpe.GenFlowTool       根据服务描述文件生成流程文件和http url mapping
    scalabpe.RenameFlowTool    对流程文件进行按目录改名

## GenSdfTool 服务描述文件xml/txt转换，格式化，修补工具

    usage: scalabpe.GenSdfTool [options] path_to_filename(xml|txt)
    options:
        -h|--help             帮助信息
        -f|--format           重新格式化xml/txt文件, 格式完毕后会删除中间文件
        -r|--repair           自动补全缺少的type定义, 默认按string类型补定义
           --reset            xml转txt时删除所有的code和id,用于准备重新对参数排序的情况
        -d|--delete           转换完毕后删除原始文件
        -c|--console          转换完毕后输出结果到控制台
        -o|--output filename  生成到指定文件中

    手工编写xml格式的服务描述文件比较麻烦; 通过此辅助工具可以在xml和等价的txt文件之间互相转换;
    txt语法的宗旨是尽可能降低编写服务描述文件的工作量

    txt文件语法：

        # 注释
        ### 多行注释, 必须以###结束注释
        service:xxx 定义服务, 必须定义服务名和服务号
        type:xxx 定义类型, 只有name必须定义, 通过array(auto或实际name)定义数组type 通过name后面直接带 #int 或 #struct 来指定类型, string为默认类型, code可自动生成
        message:xxx 定义消息, 只有消息名必须定义，id可自动生成
        sql: 表示开始sql语句定义
        req: 表示开始请求体参数列表
        res: 表示开始响应体参数列表
        其它：非以上的内容都认为是struct或message的field, field只有name必须定义，
              对结构体field, 通过name后面直接带 #int 或 #string 来指定类型,systemstring为默认类型
              对请求响应参数field, type默认为name加上_type
        通过xxx:yyy来定义属性，desc:属性必须是最后一个属性，desc:后的数据可以是任何数据, 不用考虑转义

    txt文件示例：

       service:gate    id:345

        type:fail_reason_type code:10000    desc:错误原因

        type:name_type     code:1                   
        type:quantity_type#int
        type:goodsinfo_type#struct array:auto
            name
            goods_type
            seller_id

        message:get_goods    id:1
            req:
                name                      
                quantity                      
            res:
                fail_reason                 
                list type:goodsinfo_array_type
 
        message:buy_goods
            req:
            res:

## GenSqlTool 生成db服务描述文件

    usage: scalabpe.GenSqlTool [options]
    options:
        -h|--help                   帮助信息
        -f|--file filename          从参数文件中读取命令行参数, 可以同时使用参数文件和命令行参数，命令行优先

        -t|--table tablename        指定表名
        -g|--get [params]           生成返回单条记录的select语句,params格式为 s1,s2,s3#c1=v1,c2=v2,c3, select的字段可以用*表示表的所有字段，需连接数据库
                                    params中#号前面为逗号分隔的查询列, 查询列可用空格或as来定义别名
                                    params中#号后面为逗号分隔的条件列，条件列可用=指定默认值, 否则生成:xxx这种占位符
                                    参数中若有空格，则整个参数串应使用双引号转义

        -q|--query [params]         生成返回多条记录的select语句,params格式为同get

        -i|--insert [params]        生成insert语句,params格式为 f1=v1,f2=v2,f3, 用*表示表的所有字段，需连接数据库
                                    params为用逗号分隔的更新列, 更新列可用=指定默认值,否则生成:xxx这种占位符

        -u|--update [params]        生成update语句,params格式为 f1=v1,f2=v2,f3#c1=v1,c2=v2,c3, 用*表示表的所有字段，需连接数据库
                                    params参数中#号前面为逗号分隔的更新列, 可用=指定默认值,否则生成:xxx这种占位符
                                    params参数中#号后面为逗号分隔的条件列, 可用=指定默认值,否则生成:xxx这种占位符

        -d|--delete [params]        生成delete语句,params格式为 c1=v1,c2=v2,c3
                                    params为用逗号隔开的条件列, 条件列可用=指定默认值,否则生成:xxx这种占位符

           --rowcount               生成rowcount
           --rowCount               生成rowCount
           --table_in_msg name      生成的消息名中用到的表名
           --start_code             起始code, 如果不指定，则从1开始
           --start_messageid        起始消息id, 如果不指定，则从1开始

           --url connect_url        连接串, 可选，可根据该连接串自动获得table所有字段名
           --username username      连接用户
           --password password      连接密码
           --pk name                主键对应的字段名，默认为id

           --sdf path_to_file       指定服务描述文件名
           --update_sdf             直接将结果写入sdf参数指定的服务描述文件中, 默认是输出到控制台

## GenFlowTool 根据服务描述文件生成流程文件和url mapping

    usage: scalabpe.GenFlowTool [options]
    options:
        -h|--help                       帮助信息
        -s|--sdf  servicefile           指定服务文件名
        -d|--dir  dirname               指定目录，默认为服务文件名一致
        -w|--with withname              所有流程需要继承的基类
        -i|--import                     自动加上import FlowHelper._
        -m|--mapping                    根据uri,uri2,needLogin生成url mapping
        -p|--plugin [pluginname]        -m开启时使用，可给url mapping加上plugin参数

## RenameFlowTool 对服务描述文件按指定格式进行批量改名

    usage: scalabpe.RenameFlowTool [options] dirname
    options:
        -h|--help               帮助信息
        -f|--format [format]    文件名格式，可以使用servicename,msgname,serviceid,msgid 4个特殊字符串, 默认为 msgname_serviceid_msgid

[返回](#toc)
