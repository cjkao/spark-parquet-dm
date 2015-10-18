package com.peter.model
case class Inner(a: String, b: Double)
case class Outer(key: String, inners: Seq[Inner], idx:Map[String,Int])


case class Site(id:Int, name:String, value:Double)