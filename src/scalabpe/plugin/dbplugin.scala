package scalabpe.plugin

import scalabpe.core.ArrayBufferString
import scalabpe.core.Request

// implement customed split table policy, splitTableType="custom" splitTableCustomCls="scalabpe.flow.SampleDbPlugin"
trait SplitTablePlugin {
    def generateTableIdx(req: Request): String
}

// implement customed split db policy, splitDbType="custom" splitDbCustomCls="scalabpe.flow.SampleDbPlugin"
trait SplitDbPlugin {
    def generateDbIdx(req: Request): Int
}

// usage in flow:
//
// this interface should be used only when the query result is
//
//    1) very large
//    2) rowcount unknown
//    3) cannot be paged or limited
//    4) cannot be put in memory
//
// then you can use this callback to process row by row, the sample code:
//
// val dbServiceName = "xxxxx"
// val dbServiceId = Flow.router.codecs.codecs_names.getOrElse(dbServiceName,null).serviceId
// val db = Flow.router.findActor(dbServiceId,0).asInstanceOf[BaseDbActor].db
// val callback = new QueryCallback() { ... } // implement this interface
// val sql = "xxx" // prepare the sql
// val rowCount = db.query_db(sql,db.masterList(0),callback) // this method doesnot support transaction mode
//
// rowNum: starts from 1
// buff: the row, only values
// return: true to continue, false to stop
//

trait QueryCallback {
    def process(rowNum: Int, buff: ArrayBufferString): Boolean
}

