
testcase: 

    assert:sleep  req: s=2 m=wait for seconds
    
    assert:newredis.set 
        req: key=123456789 value=123
        res: 

    assert:newredis.get 
        req: key=123456789 
        res:  value=123

#testcase: enabled:0
    assert:newredis.set 
        req: key=a value=123
        res: 

    assert:newredis.get 
        req: key=a
        res: value=123

    #assert:sleep  req: s=2 m=wait for seconds

    assert:newredis.set 
        req: key=c value=789
        res: 

    assert:newredis.get 
        req: key=c
        res: value=789

    assert:newredis.set 
        req: key=b value=456
        res: 

    assert:newredis.get 
        req: key=b
        res: value=456

    assert:sleep  req: s=5 m=wait for seconds

    assert:newredis.set 
        req: key=a value=123
        res: 

    assert:newredis.get 
        req: key=a
        res: value=123

    assert:newredis.set 
        req: key=c value=789
        res: 

    assert:newredis.get 
        req: key=c
        res: value=789

    assert:newredis.set 
        req: key=b value=456
        res: 

    assert:newredis.get 
        req: key=b
        res: value=456
        

