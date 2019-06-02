package com.atguigu.sparkmall.realtime

object test04 {
  def main(args: Array[String]): Unit = {

  }
}
class Node(int:Int){
  var pre:Node = null
  var next:Node =  null
}

class SignaList(node:Node){
  val head  = new Node(-1)
  var temp = head
  //TODO 添加元素
  def push(node: Node): Unit ={
    while (true){
      if (temp.next == null){
        temp.next= node
        node.pre= temp
      }else{
        temp = temp.next
      }
    }
  }
  //TODO 遍历元素
  def fore(): Unit ={
    
  }
}
