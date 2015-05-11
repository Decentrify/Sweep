package se.sics.ms.scenarios.general;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;
import javassist.expr.ExprEditor;
import javassist.expr.MethodCall;
import se.sics.kompics.simulation.TimeInterceptor;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

/**
 * @author Steffen Grohsschmiedt
 */
public class ThreadedTimeInterceptor extends TimeInterceptor {

    private static final String s = "se.sics.kompics.simulation.SimulatorSystem";

    private static HashSet<String> exceptions = new HashSet<String>();

    private File directory;

    public ThreadedTimeInterceptor(File directory) {
        super(directory);
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
}