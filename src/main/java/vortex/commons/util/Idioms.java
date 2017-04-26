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

import org.omg.dds.core.policy.Durability;
import org.omg.dds.core.policy.History;
import org.omg.dds.core.policy.PolicyFactory;
import org.omg.dds.core.policy.ResourceLimits;
import org.omg.dds.domain.DomainParticipant;
import org.omg.dds.pub.DataWriterQos;
import org.omg.dds.pub.Publisher;
import org.omg.dds.sub.DataReaderQos;
import org.omg.dds.sub.Subscriber;
import org.omg.dds.topic.TopicQos;

import java.util.concurrent.TimeUnit;

public class Idioms {
    private Idioms() {
    }

    public static class SoftState<T> extends Idiom<T> {
        private final int history;

        public SoftState(Class<T> type, String name, int history, DomainParticipant dp, Subscriber sub, Publisher pub) {
            super(type, dp, name, sub, pub, Durability.Kind.VOLATILE);
            this.history = history;
        }

        public SoftState(Class<T> type, String name, int history, DomainParticipant dp) {
            this(type, name, history, dp, dp.createSubscriber(), dp.createPublisher());
        }

        public SoftState(String name, Class<T> type, int history) {
            this(type, name, history, VConfig.DefaultEntities.defaultDomainParticipant(), VConfig.DefaultEntities.defaultSub(), VConfig.DefaultEntities.defaultPub());
        }

        public SoftState(String name, Class<T> type) {
            this(name, type, 1);
        }


        private static DataReaderQos readerQos(Subscriber sub, int history) {
            final PolicyFactory pf = VConfig.DefaultEntities.defaultPolicyFactory();
            return sub.getDefaultDataReaderQos().withPolicies(
                    pf.Reliability().withBestEffort(),
                    pf.Durability().withVolatile(),
                    pf.History().withKeepLast(history)
            );
        }

        private static DataReaderQos readerQos(Subscriber sub) {
            return readerQos(sub, 1);
        }

        private static DataWriterQos writerQos(Publisher pub, int history) {
            final PolicyFactory pf = VConfig.DefaultEntities.defaultPolicyFactory();

            return pub.getDefaultDataWriterQos().withPolicies(
                    pf.Reliability().withBestEffort(),
                    pf.Durability().withVolatile(),
                    pf.History().withKeepLast(history)
            );
        }

        private static DataWriterQos writerQos(Publisher pub) {
            return writerQos(pub, 1);
        }

        private static TopicQos topicQos() {
            return topicQos(VConfig.DefaultEntities.defaultDomainParticipant());
        }

        private static TopicQos topicQos(DomainParticipant dp) {
            return dp.getDefaultTopicQos();
        }

        @Override
        protected TopicQos topicQos(DomainParticipant dp, Durability.Kind durability, ResourceLimits rlimits) {
            return topicQos(dp);
        }

        @Override
        protected DataReaderQos readerQos(Subscriber sub, Durability.Kind durability) {
            return readerQos(sub, history);
        }

        @Override
        protected DataWriterQos writerQos(Publisher pub, Durability.Kind durability) {
            return writerQos(pub, history);
        }
    }

    public static class HardState<T> extends Idiom<T> {

        protected final int history;


        public HardState(String name, Class<T> type, Durability.Kind durability, int history, DomainParticipant dp, Subscriber sub, Publisher pub) {
            super(type, dp, name, sub, pub, durability);
            this.history = history;
        }

        public HardState(String name, Class<T> type, Durability.Kind durability, int history, DomainParticipant dp) {
            this(name, type, durability, history, dp, dp.createSubscriber(), dp.createPublisher());
        }

        public HardState(String name, Class<T> type, Durability.Kind durability, int history) {
            this(name, type, durability, history, VConfig.DefaultEntities.defaultDomainParticipant(), VConfig.DefaultEntities.defaultSub(), VConfig.DefaultEntities.defaultPub());
        }

        public HardState(String name, Class<T> type, Durability.Kind durability) {
            this(name, type, durability, 1);
        }


        private static DataReaderQos readerQos(Subscriber sub, Durability.Kind durability, int history) {
            final PolicyFactory pf = VConfig.DefaultEntities.defaultPolicyFactory();

            return sub.getDefaultDataReaderQos().withPolicies(
                    pf.Reliability().withReliable(),
                    pf.Durability().withKind(durability),
                    pf.History().withKeepLast(history),
                    pf.ReaderDataLifecycle().withAutoPurgeDisposedSamplesDelay(0, TimeUnit.SECONDS)
            );
        }

        protected DataReaderQos readerQos(Subscriber sub, Durability.Kind durability) {
            return readerQos(sub, durability, 1);
        }

        private static DataWriterQos writerQos(Publisher pub, Durability.Kind durability, int history) {
            final PolicyFactory pf = VConfig.DefaultEntities.defaultPolicyFactory();

            return pub.getDefaultDataWriterQos().withPolicies(
                    pf.Reliability().withReliable(),
                    pf.Durability().withKind(durability),
                    pf.History().withKeepLast(history),
                    pf.WriterDataLifecycle().withAutDisposeUnregisteredInstances(false)
            );
        }

        protected DataWriterQos writerQos(Publisher pub, Durability.Kind durability) {
            return writerQos(pub, durability, 1);
        }

        protected TopicQos topicQos(DomainParticipant dp, Durability.Kind durability, ResourceLimits rlimits) {
            final PolicyFactory pf = VConfig.DefaultEntities.defaultPolicyFactory();

            if (durability == Durability.Kind.PERSISTENT ||
                    durability == Durability.Kind.TRANSIENT) {
                return dp.getDefaultTopicQos().withPolicies(
                        pf.History().withKeepLast(1),
                        pf.Durability().withKind(durability),
                        pf.DurabilityService()
                                .withHistoryDepth(1)
                                .withHistoryKind(History.Kind.KEEP_LAST)
                                .withMaxInstances(rlimits.getMaxInstances())
                                .withMaxSamples(rlimits.getMaxSamples())
                                .withMaxSamplesPerInstance(rlimits.getMaxSamplesPerInstance()));
            } else {
                return dp.getDefaultTopicQos().withPolicies(
                        pf.History().withKeepLast(1),
                        pf.Durability().withKind(durability));
            }
        }

        private TopicQos topicQos(Durability.Kind durability, ResourceLimits rlimits) {
            return topicQos(VConfig.DefaultEntities.defaultDomainParticipant(), durability, rlimits);
        }
    }

    public static class Event<T> extends Idiom<T> {


        public Event(String name, Class<T> type, Durability.Kind durability, DomainParticipant dp, Subscriber sub, Publisher pub) {
            super(type, dp, name, sub, pub, durability);
        }

        public Event(String name, Class<T> type, Durability.Kind durability, DomainParticipant dp) {
            this(name, type, durability, dp, dp.createSubscriber(), dp.createPublisher());
        }

        public Event(String name, Class<T> type, Durability.Kind durability) {
            this(name, type, durability, VConfig.DefaultEntities.defaultDomainParticipant(), VConfig.DefaultEntities.defaultSub(), VConfig.DefaultEntities.defaultPub());
        }


        @Override
        protected TopicQos topicQos(DomainParticipant dp, Durability.Kind durability, ResourceLimits rlimits) {
            return topicQos(dp);
        }

        private static TopicQos topicQos(DomainParticipant dp) {
            return dp.getDefaultTopicQos();
        }

        protected DataReaderQos readerQos(Subscriber sub, Durability.Kind durability) {
            final PolicyFactory pf = VConfig.DefaultEntities.defaultPolicyFactory();

            return sub.getDefaultDataReaderQos().withPolicies(
                    pf.Reliability().withReliable(),
                    pf.Durability().withKind(durability),
                    pf.History().withKeepAll(),
                    pf.ReaderDataLifecycle().withAutoPurgeDisposedSamplesDelay(0, TimeUnit.SECONDS)
            );
        }

        protected DataWriterQos writerQos(Publisher pub, Durability.Kind durability) {
            final PolicyFactory pf = VConfig.DefaultEntities.defaultPolicyFactory();

            return pub.getDefaultDataWriterQos().withPolicies(
                    pf.Reliability().withReliable(),
                    pf.Durability().withKind(durability),
                    pf.History().withKeepAll(),
                    pf.WriterDataLifecycle().withAutDisposeUnregisteredInstances(false)
            );
        }
    }
}
