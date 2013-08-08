package se.sics.ms.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Kompics;
import se.sics.peersearch.types.IndexEntry;

import javax.swing.*;
import javax.swing.event.PopupMenuEvent;
import javax.swing.event.PopupMenuListener;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/7/13
 * Time: 1:05 PM
 */
public class TrayUI extends TrayIcon implements PropertyChangeListener {

    private static final Logger logger = LoggerFactory.getLogger(TrayUI.class);

    private final UiComponent component;
    private final JFrame searchFrame;
    private final SearchUi searchUi;

    public TrayUI(Image image, UiComponent component) {
        super(image);

        this.component = component;

        searchFrame = new JFrame("Search");
        searchFrame.setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
        searchUi = new SearchUi(component);
        searchFrame.setContentPane(new SearchUi(component).root);
        searchFrame.pack();

        EventQueue.invokeLater(new Runnable() {
            @Override
            public void run() {
                createGui();
            }
        });
    }

    private void createGui() {
        PopupMenu popup = new PopupMenu();
        MenuItem searchItem = new MenuItem("Search");
        searchItem.addActionListener(getSearchAction());
        MenuItem addEntry = new MenuItem("Add new entry");
        addEntry.addActionListener(getAddEntryAction());
        MenuItem exitItem = new MenuItem("Exit");
        exitItem.addActionListener(getExitAction());

        popup.add(searchItem);
        popup.add(addEntry);
        popup.add(exitItem);
        this.setPopupMenu(popup);
        try {
            SystemTray.getSystemTray().add(this);
        } catch (AWTException e) {
            e.printStackTrace();
        }
    }

    private ActionListener getSearchAction() {
        return new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                searchFrame.setVisible(true);
            }
        };
    }

    private ActionListener getAddEntryAction() {
        return new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {

            }
        };
    }

    private ActionListener getExitAction() {
        return new ActionListener() {

            @Override
            public void actionPerformed(ActionEvent e) {

                logger.info("Exit action performed by user");
                Kompics.shutdown();
                System.exit(0);
            }
        };
    }

    @Override
    public void propertyChange(PropertyChangeEvent propertyChangeEvent) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void showSearchResults(IndexEntry[] results) {
        searchUi.showSearchResults(results);
    }

}
