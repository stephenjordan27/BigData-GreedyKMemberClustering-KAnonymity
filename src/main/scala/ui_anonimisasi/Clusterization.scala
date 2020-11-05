package ui_anonimisasi

import java.awt.{Dimension, Font, Insets}

import scala.swing.{Action, BorderPanel, BoxPanel, Button, ComboBox, GridBagPanel, Label, MainFrame, Orientation, Panel, ScrollPane, SimpleSwingApplication, Swing, TabbedPane, Table, TextField}

object Clusterization extends SimpleSwingApplication {
  import TabbedPane._

  lazy val tabs = new TabbedPane {
    pages += new Page("Parameter", new GridBagPanel { grid =>
      import GridBagPanel._

      val c = new Constraints
      c.fill = Fill.Horizontal
      c.ipadx = 100
      c.weightx = 0
      c.grid = (1,0)
      c.insets = new Insets(0,-170,0,0)
      layout(new Label("Clusterization"){
        font = new Font("TimesRoman", Font.PLAIN, 36 )
      }) = c

      c.grid = (0,1)
      c.insets = new Insets(20,-10,0,0)
      layout(new Label("Dataset :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c


      val patterns = List(
        "C:\\Users\\asus\\Desktop\\output\\clean_data.csv",
        "C:\\Users\\asus\\Desktop\\output\\clean_data.csv"
      )
      c.grid = (2,1)
      c.ipadx = 60
      c.weightx = 0
      c.insets = new Insets(20,-150,0,0)
      layout(new ComboBox(patterns) {
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (0,2)
      c.insets = new Insets(20,-10,0,0)
      layout(new Label("Algorithm"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c


      val patterns2 = List(
        "Greedy K-member clustering"
      )
      c.grid = (2,2)
      c.ipadx = 80
      c.weightx = 0
      c.insets = new Insets(20,-150,0,0)
      layout(new ComboBox(patterns2) {
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c




      c.grid = (0,3)
      c.insets = new Insets(20,0,0,0);
      layout(new Label("Cluster (k) :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,3)
      c.insets = new Insets(20,-10,0,0);
      layout(new TextField("0"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c



      c.grid = (3,4)
      c.ipadx = 20
      c.weightx = 0
      c.ipady = 20
      c.weighty = 0
      c.insets = new Insets(40,0,0,0)
      layout(new Button(Action("Run ") {  })) = c
      border = Swing.EmptyBorder(5, 5, 5, 5)
    })


    pages += new Page("Table Viewer", new BoxPanel(Orientation.Vertical) {
      minimumSize_= (new Dimension(300, 1500))
      val model = Array(
        List("39", "State-gov", "Bachelors", "Adm-clerical", "Male", "<=50K", false).toArray
      )

      val table = new Table(model, Array("age", "workclass","education","occupation","sex","income")) {
        preferredViewportSize = new Dimension(0, 1500)
        font = new Font("TimesRoman", Font.PLAIN, 13 )
      }

      listenTo(table.selection)

      contents += new ScrollPane(table)

      contents += new BoxPanel(Orientation.Vertical) {
        contents += new BorderPanel {
          val button2 = new Button("Save")
          add(button2, BorderPanel.Position.South)
        }

      }




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
