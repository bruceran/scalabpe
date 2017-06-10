global: http_base_url:http://job.gcloud.com.cn/ http_method:post http_ok_field:code 
    define:   a=1

testcase:  enabled:0
    assert_http:webapi/login
        req: service_ticket=sndatest1
        res:  Location.contains(job.gcloud.com.cn)=true

    assert_http:webapi/allproj_list
        req: 
        res: total=5 data[0].proj_id=4

   assert_http:webapi/alluser_list
        req: 
        res: total=5
             data[0].userid.indexOf(7)=0

    assert_http:webapi/netdisk/script_upload
        req: file=upload_file:testcase/t.sh is_cover=upload_value:1 
        res: 


    assert_http:webapi/netdisk/script_upload
        req: file=upload_file:testcase/t.zip is_cover=upload_value:1 
        res: 

    assert_http:webapi/netdisk/script_download
        req: id=763 file=download_file:testcase/d.sh 
        res: 

    assert_http:webapi/netdisk/script_download
        req: id=764 file=download_file:testcase/d.zip 
        res: 

testcase: 
    assert_http:webapi/login
        req: service_ticket=sndatest1
        res:  Location.contains(job.gcloud.com.cn)=true

    assert_http:webapi/netdisk/upload
        req: file=upload_file:testcase/t.sh is_cover=upload_value:1 
        res: 

