package com.atguigu.sparkmall.realtime
import util.control.Breaks._

object SingleLinkedListDemo {
  def main(args: Array[String]): Unit = {
    val list: SingleLinkedList = new SingleLinkedList
    val heroNode: HeroNode = new HeroNode(1,"张三","三")
    val heroNode1: HeroNode = new HeroNode(2,"李四","四")
    val heroNode2: HeroNode = new HeroNode(6,"赵六","六")
    val heroNode3: HeroNode = new HeroNode(5,"王五","五")
    //list.add(heroNode)
    //list.add(heroNode1)

    //list.del(2)
    list.addByOrder(heroNode)
    list.addByOrder(heroNode1)
    list.addByOrder(heroNode2)
    list.addByOrder(heroNode3)
    list.fore()

  }
}
class SingleLinkedList{
  val head: HeroNode = new HeroNode(-1,"","")
  //TODO 添加元素
  def add(heroNode:HeroNode): Unit = {

    var temp = head
    breakable {
      while (true) {
        if (temp.next == null) {
          break()
        }
        temp = temp.next
      }
    }
    temp.next=heroNode
  }

  //TODO 遍历单链表
  def fore(): Unit ={
    var temp = head.next


    breakable {
      while (true) {
        if (temp != null) {
          println(temp.no,temp.name,temp.nickName)
        } else {
          break()
        }
        temp =temp.next

      }
    }
  }

  //TODO 删除元素
  def del(no:Int): Unit ={
    var temp  =head.next
    var p =temp
    breakable{
    while (true) {
      if (temp != null) {
        if (temp.no == no) {
          while (true) {
            if (p.next == temp && temp.next != null) {
              p.next = p.next.next
              break()
            } else if (p.next == temp && temp.next == null) {
              p.next = null
              break()
            } else {
              p = p.next
            }
          }
        } else {
          temp = temp.next
        }

      } else {
        break()
      }
    }


    }

  }


  //TODO 按照no编号从小到大插入
  def addByOrder(heroNode: HeroNode): Unit ={
    var temp =head
    breakable {
      while (true) {

        if (temp.next == null) {
          temp.next = heroNode
          break()
        } else if (temp.next.no > heroNode.no && temp.next != null) {
          heroNode.next = temp.next
          temp.next = heroNode
          break()
        } else {
          temp = temp.next
        }


      }
    }
  }







}







class HeroNode(hno:Int,hname:String,hnickName:String){
  val no  = hno
  val name = hname
  val nickName=hnickName
  var next:HeroNode=null
}