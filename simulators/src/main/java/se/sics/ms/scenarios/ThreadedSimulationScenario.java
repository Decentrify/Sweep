package se.sics.ms.scenarios;

import javassist.ClassPool;
import javassist.Loader;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.TimeZone;

public abstract class ThreadedSimulationScenario extends SimulationScenario {
    public final void simulate2(Class<? extends ComponentDefinition> main) {
        store();

        try {
            Loader cl = AccessController
                    .doPrivileged(new PrivilegedAction<Loader>() {
                        @Override
                        public Loader run() {
                            return new Loader();
                        }
                    });
            cl.addTranslator(ClassPool.getDefault(), new ThreadedTimeInterceptor(null));
            Thread.currentThread().setContextClassLoader(cl);
            TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
            cl.run(main.getCanonicalName(), null);
        } catch (Throwable e) {
            throw new RuntimeException("Exception caught during simulation", e);
        }
    }
}
