package ui_pengujian

import java.awt.{Dimension, Font, Insets}

import scala.swing.{Action, BorderPanel, BoxPanel, Button, FileChooser, GridBagPanel, Label, MainFrame, Orientation, Panel, ScrollPane, SimpleSwingApplication, Swing, TabbedPane, Table}

object DataInput extends SimpleSwingApplication {
  import TabbedPane._

  lazy val tabs = new TabbedPane {
    pages += new Page("File Selection", new GridBagPanel { grid =>
      import GridBagPanel._

      val c = new Constraints
      c.fill = Fill.Horizontal

      c.grid = (0,0)
      c.insets = new Insets(0,45,0,0)
      layout(new Label("Data Input"){
        font = new Font("TimesRoman", Font.PLAIN, 40 )
      }) = c

      c.grid = (0,1)
      c.insets = new Insets(20,10,0,0)
      layout(new Label("Please select a file :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (0,2)
      c.insets = new Insets(30,-55,0,0);
      layout(new Label("Open File :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,2)
      c.insets = new Insets(30,0,0,0);
      val chooser = new FileChooser
      layout(new Button(Action("Open") {
        chooser.showOpenDialog(grid)
        font = new Font("TimesRoman", Font.PLAIN, 16 )

      })) = c

      c.grid = (0,3)
      c.insets = new Insets(20,-50,0,0);
      layout(new Label("File Name :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,3)
      c.insets = new Insets(20,0,0,0);
      layout(new Label("adult100k.csv"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (0,4)
      c.insets = new Insets(20,-45,0,0);
      layout(new Label("Path Name :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (2,4)
      c.ipadx = -400
      c.weightx = 0f
      c.insets = new Insets(20,-97,0,0);
      layout(new Label("C:\\Users\\asus\\AnonymizationWithBigData\\input\\aaaaaaaaaaaaaa\naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c


      c.grid = (0,5)
      c.insets = new Insets(20,-80,0,0)
      layout(new Label("File Size :") {
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,5)
      c.insets = new Insets(20,-30,0,0)
      layout(new Label("10968 KB"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (3,6)
      c.ipadx = 40
      c.weightx = 0
      c.ipady = 20
      c.weighty = 0
      c.insets = new Insets(30,0,0,0)
      layout(new Button(Action("Add") {  })) = c
      border = Swing.EmptyBorder(5, 5, 5, 5)

      c.grid = (3,7)
      c.insets = new Insets(10,0,0,0)
      layout(new Button(Action("Clear") {  })) = c
      border = Swing.EmptyBorder(5, 5, 5, 5)
    })
    pages += new Page("Datasets", new BoxPanel(Orientation.Vertical) {
      minimumSize_= (new Dimension(300, 1500))
      val model = Array(
        List("C:\\Users\\asus\\IdeaProjects\\AnonymizationWithBigData\\input\\adult100k.csv", false).toArray,
        List("C:\\Users\\asus\\IdeaProjects\\AnonymizationWithBigData\\input\\adult100k.csv", false).toArray,
        List("C:\\Users\\asus\\IdeaProjects\\AnonymizationWithBigData\\input\\adult100k.csv", false).toArray,
        List("C:\\Users\\asus\\IdeaProjects\\AnonymizationWithBigData\\input\\adult100k.csv", false).toArray
      )

      val table = new Table(model, Array("Path", "Status")) {
        preferredViewportSize = new Dimension(0, 1500)
        font = new Font("TimesRoman", Font.PLAIN, 13 )
      }

      //1.6:table.fillsViewportHeight = true
      listenTo(table.selection)

      contents += new ScrollPane(table)
      val button2 = new Button("Delete")

      contents += new BoxPanel(Orientation.Vertical) {
        contents += new BorderPanel {
          add(new Button("Delete"), BorderPanel.Position.South)
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
