/**
 * PrismTech licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License and with the PrismTech Vortex product. You may obtain a copy of the
 * License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License and README for the specific language governing permissions and
 * limitations under the License.
 */
package vortex.commons.util;

import org.omg.dds.core.ServiceEnvironment;
import org.omg.dds.core.policy.PolicyFactory;
import org.omg.dds.domain.DomainParticipant;
import org.omg.dds.domain.DomainParticipantFactory;
import org.omg.dds.pub.Publisher;
import org.omg.dds.sub.Subscriber;

import java.util.concurrent.atomic.AtomicReference;

public final class VConfig {
    static final String SERVICE_ENVIRONMENT_PROPERTY = "dds.service.environment";
    static final String DDS_RUNTIME_PROPERTY = "dds.runtime";
    static final String DDS_DOMAIN_PROPERTY = "dds.domain";

    static final String CAFE_SERVICE_ENV = "com.prismtech.cafe.core.ServiceEnvironmentImpl";
    static final String OSPL_SERVICE_ENV = "org.opensplice.dds.core.OsplServiceEnvironment";

    static final String CAFE_RUNTIME = "cafe";
    static final String OSPL_RUNTIME = "ospl";

    public static final String DDS;
    public static final String SERVICE_ENVIRONMENT;
    public static final ServiceEnvironment ENV;
    public static final int DEFAULT_DOMAIN = 0;
    public static final int DOMAIN;

    static {
        String ddsRuntime = System.getProperty(DDS_RUNTIME_PROPERTY);
        DDS = (ddsRuntime == null) ? CAFE_RUNTIME : ddsRuntime;

        String ddsDomain = System.getProperty(DDS_DOMAIN_PROPERTY);
        int ddsDomainInt = DEFAULT_DOMAIN;
        try {
            ddsDomainInt = Integer.parseInt(ddsDomain);
        } catch (NumberFormatException e) {
            // log a warning and use default domain
        }
        DOMAIN = ddsDomainInt;

        String svcEnv = System.getProperty(SERVICE_ENVIRONMENT_PROPERTY);
        if (svcEnv == null) {
            if (DDS.equals(CAFE_RUNTIME)) {
                SERVICE_ENVIRONMENT = CAFE_SERVICE_ENV;
            } else if (DDS.equals(OSPL_RUNTIME)) {
                SERVICE_ENVIRONMENT = OSPL_SERVICE_ENV;
            } else {
                throw new RuntimeException("You need to select the DDS runtime via " + DDS_RUNTIME_PROPERTY + "=(cafe|ospl)" +
                        " or provide the service environment via the " + SERVICE_ENVIRONMENT_PROPERTY + " property.");
            }
        } else {
            SERVICE_ENVIRONMENT = svcEnv;
        }

        System.setProperty(ServiceEnvironment.IMPLEMENTATION_CLASS_NAME_PROPERTY,
                SERVICE_ENVIRONMENT);
        ENV = ServiceEnvironment.createInstance(Thread.currentThread().getContextClassLoader());
    }

    private VConfig() {
    }

    public static class DefaultEntities {
        private DefaultEntities() {
        }

        private static final AtomicReference<DomainParticipant> defaultDomainParticipant
                = new AtomicReference<>(null);
        private static final AtomicReference<Publisher> defaultPub
                = new AtomicReference<>(null);
        private static AtomicReference<Subscriber> defaultSub
                = new AtomicReference<>(null);

        private static AtomicReference<PolicyFactory> defaultPolicyFactory
                = new AtomicReference<>(null);

        public static DomainParticipant defaultDomainParticipant() {
            if (defaultDomainParticipant.get() == null) {
                DomainParticipant p = DomainParticipantFactory.getInstance(ENV).createParticipant(DOMAIN);
                if (!defaultDomainParticipant.compareAndSet(null, p)) {
                    // someone else must have created the default domain participant
                    p.close();
                }
            }

            return defaultDomainParticipant.get();
        }

        public static Publisher defaultPub() {
            if (defaultPub.get() == null) {
                Publisher pub = defaultDomainParticipant().createPublisher();
                if (!defaultPub.compareAndSet(null, pub)) {
                    // someone else must have created the default publisher
                    pub.close();
                }
            }

            return defaultPub.get();
        }

        public static Subscriber defaultSub() {
            if (defaultSub.get() == null) {
                Subscriber sub = defaultDomainParticipant().createSubscriber();
                if (!defaultSub.compareAndSet(null, sub)) {
                    // someone else must have created the default subscriber
                    sub.close();
                }
            }

            return defaultSub.get();
        }

        public static PolicyFactory defaultPolicyFactory() {
            if (defaultPolicyFactory.get() == null) {
                PolicyFactory pf = ENV.getSPI().getPolicyFactory();
                defaultPolicyFactory.compareAndSet(null, pf);
            }

            return defaultPolicyFactory.get();
        }
    }

}
