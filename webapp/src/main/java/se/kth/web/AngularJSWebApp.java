package se.kth.web;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import se.sics.ms.ui.TrayUI;

/**
 * This class launches the web application in an embedded Jetty container.
 * This is the entry point to your application. The Java command that is used for
 * launching should fire this main method.
 */
public class AngularJSWebApp implements Runnable 
{
    private boolean success = true;
    
    public AngularJSWebApp() {
    }
    
    @Override
    public void run() {

        // The simple Jetty config here will serve static content from the webapp directory
        String webappDirLocation = "src/main/webapp/";

        // The port that we should run on can be set into an environment variable
        // Look for that variable and default to 8080 if it isn't there.
        String webPort = System.getenv("PORT");
        if (webPort == null || webPort.isEmpty()) {
            webPort = TrayUI.WEB_PORT;
        }
        Server server = new Server(Integer.valueOf(webPort));

        WebAppContext webapp = new WebAppContext();
        webapp.setContextPath("/");
        webapp.setDescriptor(webappDirLocation + "/WEB-INF/web.xml");
        webapp.setResourceBase(webappDirLocation);

        server.setHandler(webapp);
        try {
            server.start();
            Logger.getLogger(AngularJSWebApp.class.getName()).log(Level.INFO, 
                    "Web Application listening at http://localhost:{0}", webPort);
        } catch (Exception ex) {
            Logger.getLogger(AngularJSWebApp.class.getName()).log(Level.SEVERE, null, ex);
            success = false;
        }
        try {            
            server.join();
        } catch (InterruptedException ex) {
            Logger.getLogger(AngularJSWebApp.class.getName()).log(Level.SEVERE, null, ex);
            success = false;
        }
    }

    public boolean isSuccess() {
        return success;
    }
    
}
