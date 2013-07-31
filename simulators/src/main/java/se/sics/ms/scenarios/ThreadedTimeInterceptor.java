package se.sics.ms.scenarios;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;
import javassist.Translator;
import javassist.expr.ExprEditor;
import javassist.expr.MethodCall;
import se.sics.kompics.simulation.TimeInterceptor;

public class ThreadedTimeInterceptor extends TimeInterceptor {

    private static final String s = "se.sics.kompics.simulation.SimulatorSystem";

    private static HashSet<String> exceptions = new HashSet<String>();

    private File directory;

    public ThreadedTimeInterceptor(File directory) {
        super(directory);
    }

    public void start(ClassPool pool) throws NotFoundException,
            CannotCompileException {

        // well known exceptions
        exceptions.add("se.sics.kompics.p2p.simulator.P2pSimulator");
        exceptions.add("org.apache.log4j.PropertyConfigurator");
        exceptions.add("org.apache.log4j.helpers.FileWatchdog");
        exceptions.add("org.mortbay.thread.QueuedThreadPool");
        exceptions.add("org.mortbay.io.nio.SelectorManager");
        exceptions.add("org.mortbay.io.nio.SelectorManager$SelectSet");
        exceptions
                .add("org.apache.commons.math.stat.descriptive.SummaryStatistics");
        exceptions
                .add("org.apache.commons.math.stat.descriptive.DescriptiveStatistics");

        // try to add user-defined exceptions from properties file
        InputStream in = ClassLoader
                .getSystemResourceAsStream("timer.interceptor.properties");
        Properties p = new Properties();
        if (in != null) {
            try {
                p.load(in);
                for (String classname : p.stringPropertyNames()) {
                    String value = p.getProperty(classname);
                    if (value != null && value.equals("IGNORE")) {
                        exceptions.add(classname);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void onLoad(ClassPool pool, final String classname)
            throws NotFoundException, CannotCompileException {

        int d = classname.indexOf("$");
        String outerClass = (d == -1 ? classname : classname.substring(0, d));

        if (exceptions.contains(outerClass)) {
            CtClass cc = pool.get(classname);
            saveClass(cc);

            return;
        }

        CtClass cc = pool.get(classname);

        // makeSerializable(pool, cc);

        cc.defrost();
        cc.instrument(new ExprEditor() {
            // redirect method calls
            @Override
            public void edit(MethodCall m) throws CannotCompileException {
                String className = m.getClassName();
                String method = m.getMethodName();
                try {
                    className = m.getMethod().getDeclaringClass().getName();
                } catch (NotFoundException e) {
                    throw new RuntimeException("Cannot instrument call to "
                            + className + "." + method + "() in " + classname,
                            e);
                }
                if (className == null || method == null) {
                    return;
                }
                // redirect calls to System.currentTimeMillis()
                if (className.equals("java.lang.System")
                        && method.equals("currentTimeMillis")) {

                    m.replace("{ $_ = " + s + ".currentTimeMillis(); }");
                }
                // redirect calls to System.nanoTime()
                if (className.equals("java.lang.System")
                        && method.equals("nanoTime")) {
                    m.replace("{ $_ = " + s + ".nanoTime(); }");
                }
                // redirect calls to Thread.sleep()
//                if (className.equals("java.lang.Thread")
//                        && method.equals("sleep")) {
//                    m.replace("{ " + s + ".sleep($$); }");
//                }
                // redirect calls to Thread.start()
//                if (className.equals("java.lang.Thread")
//                        && method.equals("start")) {
//                    m.replace("{ " + s + ".start(); }");
//                }
            }
        });

        saveClass(cc);
    }

    private void saveClass(CtClass cc) {
        if (directory != null) {
            try {
                cc.writeFile(directory.getAbsolutePath());
            } catch (CannotCompileException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // private void makeSerializable(ClassPool pool, CtClass cc)
    // throws NotFoundException {
    // boolean alreadySerializable = false;
    // CtClass parent = cc;
    //
    // // abort if class is already Serializable
    // do {
    // CtClass[] interfaces = parent.getInterfaces();
    // for (int i = 0; i < interfaces.length; i++) {
    // if (interfaces[i].getName().equals("java.io.Serializable")) {
    // alreadySerializable = true;
    // }
    // }
    // parent = parent.getSuperclass();
    // } while (parent != null);
    //
    // if (!alreadySerializable) {
    // cc.addInterface(pool.get("java.io.Serializable"));
    // }
    // }
}