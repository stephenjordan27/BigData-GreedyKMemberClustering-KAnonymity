package AnonymizationModel

object MainLCATesting {
  def main(args:Array[String]): Unit = {
    val tree:BinaryTree = new BinaryTree() // Buat binary tree secara manual
    tree.root = new Node("Person",1)
    tree.root.left = new Node("Oriental",2)
    tree.root.right = new Node("General",2)
    tree.root.left.left = new Node("Amer-Indian-Eskimo",3)
    tree.root.left.right = new Node("Asian",3)
    tree.root.right.left = new Node("White",3)
    tree.root.right.right = new Node("Black",3)
    val lca = new LowestCommonAncestor() // Buat model Lowest Common Ancestor
    val root = lca.findLCA(tree.root,"White", "Black")
    println("LCA(4, 5) : " + root)
    println("Level: " + tree.search(root).level)
  }
}
