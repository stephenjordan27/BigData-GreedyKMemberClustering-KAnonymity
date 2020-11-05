package ui_pengujian

import java.awt.{Font, Insets}

import scala.swing.FlowPanel.Alignment
import scala.swing.{Action, BorderPanel, Button, FlowPanel, GridBagPanel, Label, MainFrame, Panel, SimpleSwingApplication, TabbedPane}

object Evaluation extends SimpleSwingApplication {
  import TabbedPane._

  lazy val tabs = new TabbedPane {
    pages += new Page("Clustering", new GridBagPanel { grid =>

      import GridBagPanel._

      val c = new Constraints
      c.fill = Fill.Horizontal

      c.ipadx = 100
      c.weightx = 0
      c.grid = (0,0)
      c.insets = new Insets(0,0,0,0)
      layout(new Label("Evaluation: Silhouette Coefficient"){
        font = new Font("TimesRoman", Font.PLAIN, 30 )
      }) = c

      c.grid = (0,1)
      c.insets = new Insets(30,0,0,0)
      layout(new Label("SCORE"){
        font = new Font("TimesRoman", Font.PLAIN, 30 )
      }) = c

      c.grid = (0,2)
      c.insets = new Insets(0,0,0,0)
      layout(new Label("0.97"){
        font = new Font("TimesRoman", Font.PLAIN, 50 )
      }) = c


      c.grid = (0,3)
      c.insets = new Insets(10,0,0,0)
      layout(new FlowPanel(Alignment.Center)() {
        contents+=new Button(Action("Evaluate") {

        })

      }) = c




      c.ipadx = 100
      c.weightx = 0
      c.grid = (0,4)
      c.insets = new Insets(50,0,0,0)
      layout(new Label("Description :"){
        font = new Font("TimesRoman", Font.PLAIN, 26 )
      }) = c

      c.ipadx = 100
      c.weightx = 0
      c.grid = (0,5)
      c.insets = new Insets(0,0,0,0)
      layout(new Label("Data have been clustered in a best way"){
        font = new Font("TimesRoman", Font.PLAIN, 26 )
      }) = c



    })


    pages += new Page("Classification", new GridBagPanel { grid =>

      import GridBagPanel._

      val c = new Constraints
      c.fill = Fill.Horizontal

      c.ipadx = 100
      c.weightx = 0
      c.grid = (0,0)
      c.insets = new Insets(0,0,0,0)
      layout(new Label("Evaluation: Accuracy"){
        font = new Font("TimesRoman", Font.PLAIN, 30 )
      }) = c

      c.ipadx = 100
      c.weightx = 0
      c.grid = (0,1)
      c.insets = new Insets(30,0,0,0)
      layout(new Label("SCORE"){
        font = new Font("TimesRoman", Font.PLAIN, 30 )
      }) = c

      c.ipadx = 100
      c.weightx = 0
      c.grid = (0,2)
      c.insets = new Insets(0,0,0,0)
      layout(new Label("0.97"){
        font = new Font("TimesRoman", Font.PLAIN, 50 )
      }) = c

      c.grid = (0,3)
      c.insets = new Insets(10,0,0,0)
      layout(new FlowPanel(Alignment.Center)() {
        contents+=new Button(Action("Evaluate") {

        })

      }) = c  




      c.ipadx = 100
      c.weightx = 0
      c.grid = (0,5)
      c.insets = new Insets(50,0,0,0)
      layout(new Label("Description :"){
        font = new Font("TimesRoman", Font.PLAIN, 26 )
      }) = c

      c.ipadx = 100
      c.weightx = 0
      c.grid = (0,6)
      c.insets = new Insets(0,0,0,0)
      layout(new Label("Data have been labeled in a best way"){
        font = new Font("TimesRoman", Font.PLAIN, 26 )
      }) = c



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
