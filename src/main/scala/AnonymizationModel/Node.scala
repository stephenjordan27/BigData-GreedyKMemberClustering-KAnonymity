package AnonymizationModel

class Node(value: String,lvl: Int) {
  var name : String = value
  var left, right : Node = null
  var parent: Node = null
  var level : Int = lvl
}
