package AnonymizationModel

// https://www.geeksforgeeks.org/binary-tree-set-1-introduction/
class BinaryTree(key: String) {

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
    // height will be 0 if the node is leaf or null
    if(root == null || isLeaf()) return 0;
    //height of a node will be 1+ greater among height of right subtree and height of left subtree
    return(getMax(getHeight(root.left), getHeight(root.right)) + 1);
  }


}
