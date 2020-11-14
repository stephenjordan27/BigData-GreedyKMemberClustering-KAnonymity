package AnonymizationModel

import scala.collection.mutable.ListBuffer

class LowestCommonAncestor extends Serializable {
    var path1: ListBuffer[String] = ListBuffer[String]()
    var path2: ListBuffer[String] = ListBuffer[String]()

    // untuk Binary Tree (numeric/categorical)
    def findLCA(root: Node, n1:String, n2: String):String = {
      path1.clear()
      path2.clear()
      return findLCAInternal(root,n1,n2)
    }

    def findLCAInternal(root: Node, n1: String, n2: String):String= {
      if(!findPath(root,n1,path1) || !findPath(root,n2,path2)){
        println(if(!path1.isEmpty) "n1 is present" else "n1 is missing")
        println(if(!path2.isEmpty) "n2 is present" else "n2 is missing")
        return "not found"
      }

      import util.control.Breaks._
      var i = 0
      breakable {
        while (i < path1.size && i < path2.size) {
          if (!path1(i).equals(path2(i))) break
          i += 1
        }
      }

      return path1(i - 1)
    }

    def findPath(root:Node, n: String, path:ListBuffer[String]):Boolean={
      if (root == null) {
        return false;
      }

      path += root.name

      if (root.name == n) {
        return true;
      }

      if (root.left != null && findPath(root.left, n, path)) {
        return true;
      }

      if (root.right != null && findPath(root.right, n, path)) {
        return true;
      }

      path.remove(path.size - 1)

      return false
    }

}
