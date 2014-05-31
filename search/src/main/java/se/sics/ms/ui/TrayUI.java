package se.sics.ms.ui;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Kompics;
import se.sics.ms.types.IndexEntry;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.logging.Level;
import se.sics.gvod.config.VodConfig;

/**
 * Created with IntelliJ IDEA. User: kazarindn Date: 8/7/13 Time: 1:05 PM
 */
public class TrayUI extends TrayIcon implements PropertyChangeListener {

    private static final Logger logger = LoggerFactory.getLogger(TrayUI.class);

    private final UiComponent component;
    private final JFrame searchFrame;
    private final SearchSwingUi searchUi;
    private final JFrame addEntryFrame;
    private final AddIndexEntrySwingUi addIndexEntryUi;

    public TrayUI(Image image, UiComponent component) {
        super(image);

        this.component = component;

        searchFrame = new JFrame("Search");
        searchFrame.setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
        searchUi = new SearchSwingUi(searchFrame, false, component);
//        searchFrame.setContentPane(new SearchSwingUi(searchFrame, true, component).root);
        searchFrame.setContentPane(searchUi.getRoot());
        searchFrame.pack();

        addEntryFrame = new JFrame("Add Index Entry");
        addIndexEntryUi = new AddIndexEntrySwingUi(addEntryFrame, true, component);
        addEntryFrame.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        addEntryFrame.setContentPane(addIndexEntryUi.getRoot());
        addEntryFrame.pack();

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

        addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (!SwingUtilities.isRightMouseButton(e)) {
//                    searchFrame.setVisible(true);
                    String url = "http://localhost:9999/";
                    try {
                        java.awt.Desktop.getDesktop().browse(java.net.URI.create(url));
                    } catch (IOException ex) {
                        java.util.logging.Logger.getLogger(TrayUI.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }
        );

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
                addEntryFrame.setVisible(true);
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

    public void showAddResult(final boolean result) {
        addIndexEntryUi.showAddResult(result);
    }
}
