package com.atguigu.sparkmall.realtime
import util.control.Breaks._

object DoubleLinkedListDemo {
  def main(args: Array[String]): Unit = {

    var list = new DoubleLinkedList
    val heroNode: HeroNode2 = new HeroNode2(1,"张三","三")
    val heroNode1: HeroNode2 = new HeroNode2(2,"李四","四")
    val heroNode2: HeroNode2 = new HeroNode2(3,"王五","五")
    val heroNode3: HeroNode2 = new HeroNode2(3,"哈哈","哈")


    list.add(heroNode)
    list.add(heroNode1)
    list.add(heroNode2)
    list.update(heroNode3)
    list.fore()
  }


}
//添加，遍历，修改，删除
class DoubleLinkedList{
   val head: HeroNode2 = new HeroNode2(-1,"","")
  //TODO 添加元素
  def add( heroNode2: HeroNode2): Unit = {
    var temp = head
    breakable {
      while (true) {
        if (temp.next == null) {
          temp.next = heroNode2
          heroNode2.pre = temp
          break()
        } else {
          temp = temp.next
        }
      }
    }
  }
  //TODO 遍历元素
  def fore(): Unit ={
    var temp =head.next
    breakable {
      while (true) {
        if (temp != null) {
          println(temp.no, temp.name, temp.nickname)
        } else {
          break()
        }
        temp = temp.next
      }
    }
  }
  //TODO 删除元素
  def del(i: Int): Unit ={
    var temp = head.next
    breakable {
      while (true) {
        if (temp.no == i && temp.next != null) {
          temp.pre.next = temp.next
          temp.next.pre = temp.pre
          break()

        }else if(temp.no == i && temp.next == null){
          temp.pre.next =null
          break()
        }else {
          temp = temp.next
        }
      }
    }
  }
  //TODO 修改元素
  def update(heroNode2: HeroNode2): Unit ={
    var temp = head.next
    breakable {
      while (true) {
        if (temp == null) {
          break()
        } else if (temp.no == heroNode2.no) {
          temp.name = heroNode2.name
          temp.nickname = heroNode2.nickname
          break()

        } else {
          temp = temp.next
        }

      }
    }
  }



}
class HeroNode2(hNo:Int,hName:String,hNickName:String){
  val no = hNo
  var name = hName
  var nickname =hNickName
  var next:HeroNode2 = null
  var pre:HeroNode2 = null
}