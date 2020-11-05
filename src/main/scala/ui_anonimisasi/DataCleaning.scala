package ui_anonimisasi

import java.awt.{Dimension, Font, Insets}

import scala.swing.{Action, BorderPanel, Button, FileChooser, GridBagPanel, Label, MainFrame, Panel, SimpleSwingApplication, Swing, TabbedPane}

object DataCleaning extends SimpleSwingApplication {
  import TabbedPane._

  lazy val tabs = new TabbedPane {
    pages += new Page("Duplication", new GridBagPanel { grid =>
      import GridBagPanel._

      val c = new Constraints
      c.fill = Fill.Horizontal

      c.grid = (0,0)
      c.insets = new Insets(0,10,0,0)
      layout(new Label("Duplicates Cleaning"){
        font = new Font("TimesRoman", Font.PLAIN, 36 )
      }) = c

      c.grid = (0,1)
      c.insets = new Insets(20,-620,0,0)
      layout(new Label("Location file will saved in:"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,1)
      c.ipadx = 20
      c.weightx = 0
      c.insets = new Insets(20,10,0,0);
      val chooser = new FileChooser
      layout(new Button(Action("Open") {
        chooser.showOpenDialog(grid)
        font = new Font("TimesRoman", Font.PLAIN, 16 )

      })) = c

      c.grid = (2,2)
      c.ipadx = -500
      c.weightx = 0
      c.insets = new Insets(20,-405,0,0)
      layout(new Label("C:\\Users\\asus\\IdeaProjects\\AnonymizationWithBigData\\input\\adult100k.csv\\aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"){
        preferredSize = new Dimension(200,30)
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c




      c.grid = (0,4)
      c.insets = new Insets(50,5,0,0);
      layout(new Label("Total data before removing duplicate datas :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,4)
      c.insets = new Insets(50,0,0,0);
      layout(new Label("0"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (0,5)
      c.insets = new Insets(20,-5,0,0);
      layout(new Label("Total data after removing  duplicate datas :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,5)
      c.insets = new Insets(20,0,0,0);
      layout(new Label("0"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c



      c.grid = (0,6)
      c.insets = new Insets(20,-30,0,0);
      layout(new Label("Total data were removed permanently :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,6)
      c.insets = new Insets(20,0,0,0);
      layout(new Label("0"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c


      c.grid = (2,7)
      c.ipadx = 40
      c.weightx = 0
      c.ipady = 20
      c.weighty = 0
      c.insets = new Insets(30,100,0,0)
      layout(new Button(Action("Remove ") {  })) = c
      border = Swing.EmptyBorder(5, 5, 5, 5)
    })
    pages += new Page("Null Value", new GridBagPanel { grid =>
      import GridBagPanel._

      val c = new Constraints
      c.fill = Fill.Horizontal

      c.grid = (0,0)
      c.insets = new Insets(0,-2,0,0)
      layout(new Label("Null Value Cleaning"){
        font = new Font("TimesRoman", Font.PLAIN, 36 )
      }) = c

      c.grid = (0,1)
      c.insets = new Insets(20,-370,0,0)
      layout(new Label("Location file will saved in:"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,1)
      c.ipadx = 20
      c.weightx = 0
      c.insets = new Insets(20,10,0,0);
      val chooser = new FileChooser
      layout(new Button(Action("Open") {
        chooser.showOpenDialog(grid)
        font = new Font("TimesRoman", Font.PLAIN, 16 )

      })) = c

      c.grid = (2,2)
      c.ipadx = -800
      c.weightx = 0
      c.insets = new Insets(20,-400,0,0)
      layout(new Label("C:\\Users\\asus\\IdeaProjects\\AnonymizationWithBigData\\input\\adult100k.csv\\aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"){
        preferredSize = new Dimension(200,30)
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c




      c.grid = (0,4)
      c.insets = new Insets(50,-40,0,0);
      layout(new Label("Total data before removing null values :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,4)
      c.insets = new Insets(50,0,0,0);
      layout(new Label("0"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (0,5)
      c.insets = new Insets(20,-50,0,0);
      layout(new Label("Total data after removing  null values :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,5)
      c.insets = new Insets(20,0,0,0);
      layout(new Label("0"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c



      c.grid = (0,6)
      c.insets = new Insets(20,-40,0,0);
      layout(new Label("Total data were removed permanently :"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c

      c.grid = (1,6)
      c.insets = new Insets(20,0,0,0);
      layout(new Label("0"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c


      c.grid = (2,7)
      c.ipadx = 40
      c.weightx = 0
      c.ipady = 20
      c.weighty = 0
      c.insets = new Insets(30,100,0,0)
      layout(new Button(Action("Remove ") {  })) = c
      border = Swing.EmptyBorder(5, 5, 5, 5)
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
