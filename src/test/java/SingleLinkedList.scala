

object SingleLinkedList {
  def main(args: Array[String]): Unit = {
    val arr1 = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val arr2 = List('a', 'b', 'c', 'd', 'e', 'f', 'g')
    val head = Node(null, "head")


    var n = head
    for (i <- 1 to 10) {
      if (i != 10)
        n.nodes = Node(null, i.toString)
      else n.nodes = Node(null, "tail")
      n = n.nodes
    }
    val tail = rev(head)

    show(tail)
  }

  def show(head: Node): Unit = {
    if (head.nodes == null)
      return head
    var tail = head
    while (tail.nodes != null) {
      tail = tail.nodes
    }

    def re(node: Node): Unit = {
      if (node.nodes != null) {
        re(node.nodes)
        node.nodes.nodes = node
      }
      node.nodes = null
    }
    re(head)
    tail
  }

  def rev(head: Node): Node = {
    head
  }
}

  case class Node(var nodes: Node, data: String)

