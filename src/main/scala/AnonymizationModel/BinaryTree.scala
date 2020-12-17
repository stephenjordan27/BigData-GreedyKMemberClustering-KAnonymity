package AnonymizationModel

// https://www.geeksforgeeks.org/binary-tree-set-1-introduction/
class BinaryTree(key: String) extends Serializable {

  var root: Node = null

  def this(){
    this("")
    this.root = null
  }

  def search(data: String): Node = {
    return search(data,root);
  }

  def search(name: String, node: Node): Node = {
    if(node!=null){
      if(node.name == name){
        return node
      }
      else{
        var foundNode = search(name,node.left)
        if(foundNode == null){
          foundNode = search(name,node.right)
        }
        return foundNode
      }
    } else{
      return null
    }
  }

  def getMax(a: Int, b: Int): Int ={
    if(a>b) return a
    else return b
  }

  def isLeaf():Boolean={
    return (root.right == null && root.left ==null)
  }

  def getHeight(root: Node): Int ={
    return this.root.level-1
  }


}
