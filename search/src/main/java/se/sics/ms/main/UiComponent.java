package se.sics.ms.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.Self;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.ms.peer.SearchUiPort;
import se.sics.ms.search.SearchRequest;
import se.sics.peersearch.types.SearchPattern;

import javax.swing.*;
import java.awt.*;
import java.net.URL;


/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/7/13
 * Time: 12:35 PM
 */
public class UiComponent extends ComponentDefinition {
    private Logger logger = LoggerFactory.getLogger(UiComponent.class);
    Positive<Timer> timer = positive(Timer.class);
    Positive<SearchUiPort> uiPort = positive(SearchUiPort.class);

    private Self self;
    private UiComponent component;
    private TrayUI trayUI;

    public UiComponent(){
        subscribe(uiComponentInitHandler, control);
        component = this;
    }

    final Handler<UiComponentInit> uiComponentInitHandler = new Handler<UiComponentInit>() {
        @Override
        public void handle(UiComponentInit uiComponentInit) {
            self = uiComponentInit.getPeerSelf();


            if (!SystemTray.isSupported()) {
                throw new IllegalStateException("SystemTray is not supported");
            }


            trayUI = new TrayUI(createImage("search.png", "tray icon"),
                    component);
        }
    };

    protected static Image createImage(String path, String description) {
        URL imageURL = UiComponent.class.getResource(path);

        if (imageURL == null) {
            System.err.println("Resource not found: " + path);
            return null;
        } else {
            return (new ImageIcon(imageURL, description)).getImage();
        }
    }

    public void search(SearchPattern pattern) {
        trigger(new SearchRequest(pattern), uiPort);
    }
}
