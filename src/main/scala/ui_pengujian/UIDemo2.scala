package ui_pengujian

import java.awt.Font

import scala.swing.event.{SelectionChanged, ValueChanged}
import scala.swing.{Action, BorderPanel, ButtonGroup, CheckMenuItem, Dimension, ListView, MainFrame, Menu, MenuBar, MenuItem, Orientation, RadioMenuItem, ScrollPane, Separator, SimpleSwingApplication, Slider, SplitPane, TabbedPane}

object UIDemo2 extends SimpleSwingApplication {
  def top = new MainFrame {
    title = "Perangkat Lunak Pengujian"
    preferredSize_= (new Dimension(1024, 868))
    /*
     * Create a menu bar with a couple of menus and menu items and
     * set the result as this frame's menu bar.
     */
    menuBar = new MenuBar {
      contents += new Menu("Project") {
          contents += new MenuItem("An item")
          contents += new MenuItem(Action("An action item") {
            println("Action '"+ title +"' invoked")
          })
          contents += new Separator
          contents += new CheckMenuItem("Check me")
          contents += new CheckMenuItem("Me too!")
          contents += new Separator
          val a = new RadioMenuItem("a")
          val b = new RadioMenuItem("b")
          val c = new RadioMenuItem("c")
          val mutex = new ButtonGroup(a,b,c)
          contents ++= mutex.buttons
          font = new Font("TimesRoman", Font.PLAIN, 16 )
      }
      contents += new Menu("History"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }
      contents += new Menu("Help"){
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }

    }

    /*
     * The root component in this frame is a panel with a border layout.
     */
    contents = new BorderPanel {
      import BorderPanel.Position._

      var reactLive = false

      val tabs = new TabbedPane {
        displayable
        import TabbedPane._
        pages += new Page("Data Input", DataInput.ui)
        pages += new Page("Data Selection", DataSelection.ui)
        pages += new Page("Data Cleaning", DataCleaning.ui)
        pages += new Page("Data Mining", DataMining.ui)
        pages += new Page("Evaluation", Evaluation.ui)
        pages += new Page("Output", Output.ui)
        font = new Font("TimesRoman", Font.PLAIN, 16 )

      }

      val list = new ListView(tabs.pages) {
        selectIndices(0)
        selection.intervalMode = ListView.IntervalMode.Single
        renderer = ListView.Renderer(_.title)
        font = new Font("TimesRoman", Font.PLAIN, 16 )
      }
      val center = new SplitPane(Orientation.Vertical, new ScrollPane(list), tabs) {
        oneTouchExpandable = true
        continuousLayout = true
      }
      layout(center) = Center

      /*
       * This slider is used above, so we need lazy initialization semantics.
       * Objects or lazy vals are the way to go, but objects give us better
       * type inference at times.
       */
      object slider extends Slider {
        min = 0
        value = tabs.selection.index
        max = tabs.pages.size-1
        majorTickSpacing = 1
      }
      layout(slider) = South

      /*
       * Establish connection between the tab pane, slider, and list view.
       */
      listenTo(slider)
      listenTo(tabs.selection)
      listenTo(list.selection)
      reactions += {
        case ValueChanged(`slider`) =>
          if(!slider.adjusting || reactLive) tabs.selection.index = slider.value
        case SelectionChanged(`tabs`) =>
          slider.value = tabs.selection.index
          list.selectIndices(tabs.selection.index)
        case SelectionChanged(`list`) =>
          if (list.selection.items.length == 1)
            tabs.selection.page = list.selection.items(0)
      }
    }
  }
}
