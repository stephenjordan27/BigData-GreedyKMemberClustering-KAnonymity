package ui_pengujian

import java.awt.Font

import org.jfree.chart.ChartPanel

import scala.swing.FlowPanel.Alignment
import scala.swing.{BorderPanel, Component, Dimension, FlowPanel, GridBagPanel, Insets, Label, MainFrame, Panel, SimpleSwingApplication, TabbedPane}

object Output extends SimpleSwingApplication  {
  import scala.swing.TabbedPane._

  lazy val tabs = new TabbedPane {
    pages += new Page("Parameter", new GridBagPanel { grid =>
      import GridBagPanel._

      val c = new Constraints
      c.fill = Fill.Horizontal

      c.grid = (0,0)
      c.insets = new Insets(0,0,0,0)
      layout(new Label("DATA IS USEFUL"){
        font = new Font("TimesRoman", Font.PLAIN, 36 )
      }) = c

      // https://www.scala-lang.org/old/node/1069
      // http://www.jfree.org/forum/viewtopic.php?t=27606
      import org.jfree.chart.{ChartFactory, JFreeChart}
      import org.jfree.data.general.DefaultPieDataset

      val dataset = new DefaultPieDataset
      dataset.setValue("Section 1", 46.7)
      dataset.setValue("Section 2", 56.5)
      dataset.setValue("Section 3", 43.3)
      dataset.setValue("Section 4", 11.1)

      val chart: JFreeChart = ChartFactory.createPieChart("", dataset, false, false, false)

      c.grid = (0,1)
      c.insets = new Insets(0,0,0,0)
      layout(new FlowPanel(Alignment.Left)() {
        contents+= Component.wrap(new ChartPanel(chart){
          setPreferredSize(new Dimension(300,200))
        })
      }) = c


      c.grid = (0,2)
      c.insets = new Insets(0,0,0,0)
      layout(new Label("data have been anonymized"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c


      c.grid = (0,3)
      c.insets = new Insets(0,0,0,0)
      layout(new Label("data haven't been anonymized"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c


      c.grid = (0,4)
      c.insets = new Insets(30,0,0,0)
      layout(new Label("DATA IS CLEANED"){
        font = new Font("TimesRoman", Font.PLAIN, 36 )
      }) = c

      // http://www.jfree.org/forum/viewtopic.php?t=27606
      import org.jfree.chart.{ChartFactory, JFreeChart}
      import org.jfree.data.general.DefaultPieDataset

      val dataset2 = new DefaultPieDataset
      dataset.setValue("Section 1", 46.7)
      dataset.setValue("Section 2", 56.5)
      dataset.setValue("Section 3", 43.3)
      dataset.setValue("Section 4", 11.1)

      val chart2: JFreeChart = ChartFactory.createPieChart("", dataset, false, false, false)

      c.grid = (0,5)
      c.insets = new Insets(0,0,0,0)
      layout(new FlowPanel(Alignment.Left)() {
        contents+= Component.wrap(new ChartPanel(chart2){
          setPreferredSize(new Dimension(300,200))
        })
      }) = c



      c.grid = (0,6)
      c.insets = new Insets(0,0,0,0)
      layout(new Label("data deleted due to duplication"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }) = c


      c.grid = (0,7)
      c.insets = new Insets(0,0,0,0)
      layout(new Label("data deleted due to null values"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
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
