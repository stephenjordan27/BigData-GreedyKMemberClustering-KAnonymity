package AnonymizationModel

class Node(value: String,lvl: Int) extends Serializable {
  var name : String = value
  var left, right : Node = null
  var parent: Node = null
  var level : Int = lvl
}
