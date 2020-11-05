package ui_pengujian

import java.awt.{Dimension, Font}

import scala.swing.FlowPanel.Alignment
import scala.swing.{BorderPanel, BoxPanel, Button, ComboBox, FlowPanel, Label, MainFrame, Orientation, Panel, ScrollPane, SimpleSwingApplication, TabbedPane, Table}


object DataSelection extends SimpleSwingApplication {
  import scala.swing.TabbedPane._

  lazy val tabs = new TabbedPane {
    pages += new Page("Column Selection", new BoxPanel(Orientation.Vertical) {

      contents += new FlowPanel(Alignment.Left)() {
        val field = new Label("Path name file input :") {

        }
        contents += field
        val patterns = List(
          "C:\\Users\\asus\\IdeaProjects\\AnonymizationWithBigData\\input\\adult100k.csv",
          "C:\\Users\\asus\\IdeaProjects\\AnonymizationWithBigData\\input\\adult100k.csv"
        )

        val dateBox = new ComboBox(patterns) {
          preferredSize = new Dimension(700,20)
        }
        contents += dateBox

      }

      val model = Array(
        List("age", false).toArray,
        List("workclass", false).toArray,
        List("education", false).toArray,
        List("yearofeducation", false).toArray
      )

      val table = new Table(model, Array("Column Name", "Status")) {
        preferredViewportSize = new Dimension(0, 1500)
        font = new Font("TimesRoman", Font.PLAIN, 13 )
      }

      //1.6:table.fillsViewportHeight = true
      listenTo(table.selection)
      contents += new ScrollPane(table)
      contents += new BorderPanel {
        val button2 = new Button("Confirm")
        add(button2, BorderPanel.Position.South)
      }
    })
    pages += new Page("Table Viewer", new BoxPanel(Orientation.Vertical) {

      val model = Array(
        List("39", "State-gov", "Bachelors", "Adm-clerical", "Male", "<=50K", false).toArray
      )

      val table = new Table(model, Array("age", "workclass","education","occupation","sex","income")) {
        font = new Font("TimesRoman", Font.PLAIN, 13 )
        preferredViewportSize = new Dimension(0, 1500)
      }

      //1.6:table.fillsViewportHeight = true
      
      contents += new ScrollPane(table)

    })
    font = new Font("TimesRoman", Font.PLAIN, 16 )
  }

  lazy val ui: Panel = new BorderPanel {
    layout(tabs) = BorderPanel.Position.Center
  }


  lazy val top = new MainFrame {
    title = "Dialog Demo"
    contents = ui
  }
}
