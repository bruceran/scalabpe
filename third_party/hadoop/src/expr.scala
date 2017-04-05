package scalabpe.plugin.hadoop

import scalabpe.core._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.filter._
import java.util.regex._

trait FilterExpr {
    def eval(cf:String,resInfos:ArrayBuffer[ColumnInfo]):Filter
}
class FilterExprOr extends FilterExpr {
    val buff = new ArrayBuffer[FilterExpr]()
    def eval(cf:String,resInfos:ArrayBuffer[ColumnInfo]):Filter = {
        val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
        for( e <- buff ) {
            filterList.addFilter( e.eval(cf,resInfos) ) 
        }
        filterList
    }
}
class FilterExprAnd extends FilterExpr {
    val buff = new ArrayBuffer[FilterExpr]()
    def eval(cf:String,resInfos:ArrayBuffer[ColumnInfo]):Filter = {
        val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
        for( e <- buff ) {
            filterList.addFilter( e.eval(cf,resInfos) ) 
        }
        filterList
    }
}
class FilterExprNot(val expr:FilterExpr) extends FilterExpr {
    def eval(cf:String,resInfos:ArrayBuffer[ColumnInfo]):Filter = {
        throw new Exception("operator (!) not supported in filter expr")
    }
}
class FilterExprSimple(val keys:String,val operator:String,val value:String) extends FilterExpr with Logging {

    def eval(cf:String,resInfos:ArrayBuffer[ColumnInfo]):Filter = {
        try {
            evalInternal(cf,resInfos)
        } catch {
            case e:Throwable =>
                log.error("expr eval exception, e="+e.getMessage+",value="+value)
                null
        }
    }

    def genColumnInfo(defaultColumnFamily:String,fieldName:String):Tuple2[String,String] = {
        val p = fieldName.indexOf(".")
        if( p >= 0 ) {
            val cf = fieldName.substring(0,p)
            val cn = fieldName.substring(p+1)
            return ( cf,cn )
        } else {
            return ( defaultColumnFamily,fieldName)
        }
    }

    def evalInternal(dcf:String,resInfos:ArrayBuffer[ColumnInfo]):Filter = {

        val (cf,cn) =  genColumnInfo(dcf,keys)
        val compare = operator match {
            case ">" =>
                CompareFilter.CompareOp.GREATER
            case ">=" =>
                CompareFilter.CompareOp.GREATER_OR_EQUAL
            case "<" =>
                CompareFilter.CompareOp.LESS
            case "<=" =>
                CompareFilter.CompareOp.LESS_OR_EQUAL
            case "!=" | "<>" =>
                CompareFilter.CompareOp.NOT_EQUAL
            case "=" | "==" =>
                CompareFilter.CompareOp.EQUAL
            case "~=" | "#=" =>
                CompareFilter.CompareOp.EQUAL
            case "!~" | "!#" =>
                CompareFilter.CompareOp.NOT_EQUAL
            case _ =>
                throw new Exception("unknown_operator")
                
        }
        operator match {
            case "~=" | "!~" =>
                val reg = new RegexStringComparator(value)
                val f = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(cn), compare, reg); 
                f.setFilterIfMissing(true)
                f
            case "#=" | "!#"=>
                val reg = new RegexStringComparator(value, Pattern.CASE_INSENSITIVE)
                val f = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(cn), compare, reg); 
                f.setFilterIfMissing(true)
                f
            case _ =>
                val f = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(cn), compare, Bytes.toBytes(value)); 
                f.setFilterIfMissing(true)
                f
        }
    }
}
/*
class FilterExprInclude(override val keys:String,override val operator:String,override val value:String,val set: HashSet[String]) extends FilterExprSimple(keys,operator,value) with Logging {

    override def evalInternal(data:HashMapStringAny):Boolean = {

        val testValue = getTestValue(data,keys)

        operator match {
            case "in" =>
                set.contains(testValue)
            case "not_in" =>
                !set.contains(testValue)
            case "match" =>
                val testValues = genPatterns(testValue)
                for( t <- testValues ) {
                    if( set.contains(t) ) return true
                }
                false
            case "not_match" =>
                val testValues = genPatterns(testValue)
                for( t <- testValues ) {
                    if( set.contains(t) ) return false
                }
                true
            case _ =>
                false
        }
    }
}
*/
class FilterExprBuilder {

    var or: FilterExprOr = _
    var and: FilterExprAnd = _
    var not: Boolean = false
    var orand: Boolean = false
    var last: FilterExpr = _
    
    def addOr() {
        if( not ) throw new RuntimeException("'!' operator before '||'")
        if( last == null ) throw new RuntimeException("'||' operator not valid")
        if( or == null ) or = new FilterExprOr
        if( last != null ) {
            if( and != null ) { 
               and.buff += last
               last = null
               or.buff += and
               and = null
            } else {
                or.buff += last
                last = null
            }
        }
        if( and != null ) {
            or.buff += and 
            and = null
        }
        orand = true
    }
    def addAnd() {
        if( not ) throw new RuntimeException("'!' operator before '&&'")
        if( last == null ) throw new RuntimeException("'&&' operator not valid")
        if( and == null ) and = new FilterExprAnd
        and.buff += last
        last = null
        orand = true
    }
    def addNot() {
        if( not ) throw new RuntimeException("'!' operator before '!'")
        not = true
    }
    def add(e:FilterExpr) {
        if( last != null ) throw new RuntimeException("expr duplicated")
        if( not )
            last = new FilterExprNot(e)
        else
            last = e
        not = false
        orand = false
    }
    def get():FilterExpr = {
        if( not ) throw new RuntimeException("'!' operator is last")
        if( orand ) throw new RuntimeException(" && || operator is last")
  
        if( last != null ) {
            if( and != null ) { 
               and.buff += last
               last = null
           } else if( or != null ) { 
               or.buff += last
               last = null
            } else {
                return last
            }
        }
        if( and != null ) {
            if( or != null ) { 
               or.buff += and
               return or
            } else {
               return and
            }
        }
        if( or != null ) {
            return or
        }
    
        throw new RuntimeException("not valid expr")
    }
}


object FilterExprParser extends Logging {

    val reg1 = """^([a-zA-Z0-9._]+)( +[a-zA-Z_]+ +) *(.+)$""".r
    val reg2 = """^([a-zA-Z0-9._]+)( *== *| *!= *| *<> *| *>= *| *<= *| *!~ *| *!# *| *~= *| *#= *) *(.+)$""".r
    val reg3 = """^([a-zA-Z0-9._]+)( *= *| *< *| *> *) *(.+)$""".r

    def parse(s:String):FilterExpr = {
        val ns = s.replace("\n"," ").replace("\r"," ").replace("\t"," ")
        val chs = ns.toCharArray()

        try {
            parse(chs,0,chs.size)
        } catch {
            case e:Throwable =>
                log.error("filter expr not valid: expr="+s)
                null
        }
    }

    def parse(chs:Array[Char],s:Int,e:Int):FilterExpr = {
        var i = s
        val builder = new FilterExprBuilder()
        while( i < e ) {
            chs(i) match {
                case '|' =>
                    if( i + 1 >= e ) throw new RuntimeException("'||' not valid")
                    i += 1
                    if( chs(i) != '|') throw new RuntimeException("'||' not valid")
                    builder.addOr()
                case '&' =>
                    if( i + 1 >= e ) throw new RuntimeException("'&&' not valid")
                    i += 1
                    if( chs(i) != '&') throw new RuntimeException("'&&' not valid")
                    builder.addAnd()
                case '!' =>
                    builder.addNot()
                case '(' =>
                    val p = findMatchBracket(chs,i,e)
                    if( p < 0 ) throw new RuntimeException("() not match ") 
                    val expr = parse(chs,i+1,p-1)
                    builder.add(expr)
                    i = p
                case _ =>
                    val p = findExprEnd(chs,i,e)
                    if( p < 0 ) throw new RuntimeException("expr not end") 
                    val expr = parseSimple(new String(chs,i,p-i+1))
                    if( expr != null )
                        builder.add(expr)
                    i = p
            
            }
            i = i + 1
        }
        builder.get()
    }

    def findMatchBracket(chs:Array[Char],s:Int,e:Int):Int = {
        var i = s
        var lvl = 0
        var inQuota = false
        while( i < e ) {
            chs(i) match {
                case '\"' => 
                     inQuota = !inQuota
                case '(' => 
                     if( !inQuota )
                        lvl += 1
                case ')' => 
                     if( !inQuota ) {
                        lvl -= 1
                        if( lvl == 0 ) return i
                     }
                case _ => 
            }
            i += 1
        }
        -1
    }

    def findExprEnd(chs:Array[Char],s:Int,e:Int):Int = {
        var i = s
        var inQuota = false
        while( i < e ) {
            chs(i) match {
                case '\"' => 
                     inQuota = !inQuota
                case '(' | ')' | '|' | '&' | '!' => 
                     if( !inQuota ) {
                         if( chs(i) == '!' ) {
                             if( i < e && chs(i+1) != '=' ) return i-1
                         } else {
                             return i-1
                         }
                     }
                case _ => 
            }
            i += 1
        }
        e - 1
    }


    def parseSimple(s:String):FilterExpr = {
        s.trim match {
            case null | "" => null
            case reg1(keys,operator,value) =>
                val o = operator.trim.toLowerCase
                var v = value.trim
                o match {
                    //case "in" | "not_in" | "match" | "not_match" =>
                        //val set = parseArray(v)
                        //return new FilterExprInclude(keys,o,v,set)
                    case _ =>
                        throw new RuntimeException("expr not valid, expr="+s) 
                }
            case reg2(keys,operator,value) =>
                val o = operator.trim.toLowerCase
                var v = value.trim
                o match {
                    case "!=" | "==" | ">=" | "<=" | "<>" | "!~" | "!#"  | "~=" | "#=" =>
                        v = removeQuota(v)
                        return new FilterExprSimple(keys,o,v)
                    case _ =>
                        throw new RuntimeException("expr not valid, expr="+s) 
                }
            case reg3(keys,operator,value) =>
                val o = operator.trim.toLowerCase
                var v = value.trim
                o match {
                    case "=" | ">" | "<" =>
                        v = removeQuota(v)
                        return new FilterExprSimple(keys,o,v)
                    case _ =>
                        throw new RuntimeException("expr not valid, expr="+s) 
                }
            case _ => 
                throw new RuntimeException("expr not valid, expr="+s) 

        }
        null
    }
/*
    def parseArray(s:String):HashSet[String] = {
        if( !s.startsWith("[")) 
            throw new RuntimeException("array not valid, expr="+s) 
        if( !s.endsWith("]")) 
            throw new RuntimeException("array not valid, expr="+s) 
        val ns = s.substring(1,s.length-1)
        val ss = ns.split(" ") 
        val set = HashSet[String]()
        for( s <- ss if s != "") {
            val ts = removeQuota(s)
            if( ts != "")
                set.add(ts)
        }
        set
    }
*/

    def removeQuota(s:String):String = {
        if( s.startsWith("\"")) {
            if( s.endsWith("\"")) {
                return s.substring(1,s.length-1)
            }
            throw new RuntimeException("quota string not valid, expr="+s) 
        }
        return s
    }
}


