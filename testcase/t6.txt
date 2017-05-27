global: http_base_url:http://localhost:8088/ http_method:post http_rest:false http_ok_field:return_code http_ok_value:0 
    define:   a=1

testcase: enabled:1
    setup_http:test/updateUserInfo  
        req: userId=1 name=ran height=170 intro=abc 

    assert_http:test/queryUserInfo id:c1
        req: userId=1  
        res: 
            data.name=ran 
            data.height=170 
            data.intro=abc
            data.hobbies.size()=3
            data.shippingtos[0].city=上海

    assert_http:test/queryUserInfo/${a}/${c1.res.data.name}
        req: userId=1  
        res: 
            data.name=ran
            data.height=170 
            data.intro=abc
            data.hobbies.size()=3
            data.shippingtos[0].city=上海

