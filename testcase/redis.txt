

testcase:  enabled:0
    assert:newredis.set 
        req: key=a value=123
        res: 

    assert:newredis.get 
        req: key=a
        res: value=123


testcase: enabled:0 
    assert:newredis.del 
        req: key=site 
        res: 

    assert:newredis.zadd 
        req: key=site score=1 member=qq.com
        res: count=1

    assert:newredis.zcard 
        req: key=site 
        res: count=1

    assert:newredis.zaddmulti 
        req: key=site scoremembers=[{"score":2,"member":"baidu.com"},{"score":"1.5","member":"weibo.com"},{"score":"5","member":"sina.com"}]
        res: count=3

    assert:newredis.zcard 
        req: key=site 
        res: count=4

    assert:newredis.zcount 
        req: key=site  min=1 max=2
        res: count=3

    assert:newredis.zincrby
        req: key=site score=1 member=qq.com
        res: score=2

    assert:newredis.zscore
        req: key=site member=qq.com
        res: score=2

    assert:newredis.zrank
        req: key=site member=weibo.com
        res: rank=0

    assert:newredis.zrank
        req: key=site member=sina.com
        res: rank=3

    assert:newredis.zlexcount 
        req: key=site  min=- max=+
        res: count=4

    assert:newredis.zrem 
        req: key=site  member=a.com
        res: count=0

    assert:newredis.zrem 
        req: key=site  member=sina.com
        res: count=1

    assert:newredis.zremmulti 
        req: key=site  members=["baidu.com","weibo.com"]
        res: count=2

testcase: enabled:0 
    assert:newredis.del 
        req: key=site 
        res: 

    assert:newredis.zaddmulti 
        req: key=site scoremembers=[{"score":1,"member":"qq.com"},{"score":2,"member":"baidu.com"},{"score":"1.5","member":"weibo.com"},{"score":"5","member":"sina.com"}]
        res: count=4

    assert:newredis.zremrangebyrank 
        req: key=site  start=0 stop=1
        res: count=2

    assert:newredis.zrank
        req: key=site member=baidu.com
        res: rank=0

    assert:newredis.zrank
        req: key=site member=sina.com
        res: rank=1

    assert:newredis.zaddmulti 
        req: key=site scoremembers=[{"score":1,"member":"qq.com"},{"score":2,"member":"baidu.com"},{"score":"1.5","member":"weibo.com"},{"score":"5","member":"sina.com"}]
        res: 

    assert:newredis.zremrangebyscore
        req: key=site  min=2 max=5
        res: count=2

testcase:  
    assert:newredis.del 
        req: key=site 
        res: 

    assert:newredis.zaddmulti 
        req: key=site scoremembers=[{"score":1,"member":"qq.com"},{"score":2,"member":"baidu.com"},{"score":"1.5","member":"weibo.com"},{"score":"5","member":"sina.com"}]
        res: count=4

    assert:newredis.zrange
        req: key=site start=0 stop=2
        res: members.size()=3  members[0]=qq.com

    assert:newredis.zrevrange
        req: key=site start=0 stop=2
        res: members.size()=3  members[0]=sina.com

    assert:newredis.zrange
        req: key=site start=0 stop=2 withscores=1
        res: scoremembers.size()=3  scoremembers[0].member=qq.com

    assert:newredis.zrevrange
        req: key=site start=0 stop=2 withscores=1
        res: scoremembers.size()=3  scoremembers[0].member=sina.com

    assert:newredis.zrangebyscore
        req: key=site min=1.3 max=2
        res: members.size()=2  members[0]=weibo.com

    assert:newredis.zrevrangebyscore
        req: key=site min=1.3 max=2
        res: members.size()=2  members[0]=baidu.com

    assert:newredis.zrangebyscore
        req: key=site min=1.3 max=2 withscores=1
        res: scoremembers.size()=2  scoremembers[0].member=weibo.com

    assert:newredis.zrevrangebyscore
        req: key=site min=1.3 max=2 withscores=1
        res: scoremembers.size()=2  scoremembers[0].member=baidu.com



    assert:newredis.zrangebyscore
        req: key=site min=1.3 max=2 offset=0 count=1
        res: members.size()=1  members[0]=weibo.com

    assert:newredis.zrevrangebyscore
        req: key=site min=1.3 max=2 offset=0 count=1
        res: members.size()=1  members[0]=baidu.com

    assert:newredis.zrangebyscore
        req: key=site min=1.3 max=2 withscores=1 offset=0 count=1
        res: scoremembers.size()=1  scoremembers[0].member=weibo.com

    assert:newredis.zrevrangebyscore
        req: key=site min=1.3 max=2 withscores=1 offset=0 count=1
        res: scoremembers.size()=1  scoremembers[0].member=baidu.com

    assert:newredis.zrangebylex
        req: key=site min=- max=+
        res: members.size()=4

    assert:newredis.zrangebylex
        req: key=site min=- max=+ offset=2 count=2
        res: members.size()=2 members[0]=baidu.com
