package se.kth.web;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

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
            webPort = "9999";
        }
        Server server = new Server(Integer.valueOf(webPort));

        WebAppContext webapp = new WebAppContext();
        webapp.setContextPath("/");
        webapp.setDescriptor(webappDirLocation + "/WEB-INF/web.xml");
        webapp.setResourceBase(webappDirLocation);

        server.setHandler(webapp);
        try {
            server.start();
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
