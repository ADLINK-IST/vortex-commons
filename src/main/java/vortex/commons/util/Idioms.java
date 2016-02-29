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
import org.omg.dds.pub.DataWriter;
import org.omg.dds.pub.DataWriterQos;
import org.omg.dds.pub.Publisher;
import org.omg.dds.sub.DataReader;
import org.omg.dds.sub.DataReaderQos;
import org.omg.dds.sub.Subscriber;
import org.omg.dds.topic.Topic;
import org.omg.dds.topic.TopicQos;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static vortex.commons.util.LambdaExceptionUtil.rethrowConsumer;

public class Idioms {
    private Idioms() {
    }

    public static class SoftState<T> {
        private final Class<T> type;
        private final String name;
        private final int history;
        private final DomainParticipant dp;
        private final Subscriber sub;
        private final Publisher pub;

        private final AtomicReference<Topic<T>> topic
                = new AtomicReference<>(null);
        private final AtomicReference<DataReader<T>> reader
                = new AtomicReference<>(null);
        private final AtomicReference<DataWriter<T>> writer
                = new AtomicReference<>(null);

        public SoftState(Class<T> type, String name, int history, DomainParticipant dp, Subscriber sub, Publisher pub) {
            this.type = type;
            this.name = name;
            this.history = history;
            this.dp = dp;
            this.sub = sub;
            this.pub = pub;
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


        public List<T> take() {
            final ArrayList<T> result = new ArrayList<>();
            reader().select().dataState(DataState.ALL_DATA.state()).take().forEachRemaining(next -> result.add(next.getData()));
            return result;
        }

        public void write(T t) throws TimeoutException {
            writer().write(t);
        }

        public void write(List<T> t) {
            t.forEach(rethrowConsumer(next -> writer().write(next)));
        }

        private Topic<T> topic() {
            if (topic.get() == null) {
                Topic t = dp.createTopic(name, type, topicQos(dp), null);
                if (!topic.compareAndSet(null, t)) {
                    // someone else must have created the  topic
                    t.close();
                }
            }
            return topic.get();
        }

        private DataReader<T> reader() {
            if (reader.get() == null) {
                DataReader<T> dr = sub.createDataReader(topic(), readerQos(sub, history));
                if (!reader.compareAndSet(null, dr)) {
                    // someone else must have created the  topic
                    dr.close();
                }
            }
            return reader.get();
        }

        private DataWriter<T> writer() {
            if (writer.get() == null) {
                DataWriter<T> dw = pub.createDataWriter(topic(), writerQos(pub, history));
                if (!writer.compareAndSet(null, dw)) {
                    // someone else must have created the  topic
                    dw.close();
                }
            }
            return writer.get();
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
    }

    public static class HardState<T> {
        private final Class<T> type;
        private final String name;
        private final int history;
        private final Durability.Kind durability;
        private final ResourceLimits rlimits;
        private final DomainParticipant dp;
        private final Subscriber sub;
        private final Publisher pub;

        private final AtomicReference<Topic<T>> topic
                = new AtomicReference<>(null);
        private final AtomicReference<DataReader<T>> reader
                = new AtomicReference<>(null);
        private final AtomicReference<DataWriter<T>> writer
                = new AtomicReference<>(null);

        public HardState(String name, Class<T> type, Durability.Kind durability, int history, DomainParticipant dp, Subscriber sub, Publisher pub) {
            this.type = type;
            this.name = name;
            this.history = history;
            this.durability = durability;
            this.dp = dp;
            this.sub = sub;
            this.pub = pub;
            this.rlimits = VConfig.DefaultEntities.defaultPolicyFactory().ResourceLimits().withMaxSamples(1).withMaxInstances(1024).withMaxSamplesPerInstance(1024 * 1024);
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


        public List<T> take() {
            final ArrayList<T> result = new ArrayList<>();
            reader().select().dataState(DataState.ALL_DATA.state()).take().forEachRemaining(next -> result.add(next.getData()));
            return result;
        }

        public void write(T t) throws TimeoutException {
            writer().write(t);
        }

        public void write(List<T> t) {
            t.forEach(rethrowConsumer(next -> writer().write(next)));
        }

        private Topic<T> topic() {
            if (topic.get() == null) {
                Topic t = dp.createTopic(name, type, topicQos(dp, durability, rlimits), null);
                if (!topic.compareAndSet(null, t)) {
                    // someone else must have created the  topic
                    t.close();
                }
            }
            return topic.get();
        }

        private DataReader<T> reader() {
            if (reader.get() == null) {
                DataReader<T> dr = sub.createDataReader(topic(), readerQos(sub, durability, history));
                if (!reader.compareAndSet(null, dr)) {
                    // someone else must have created the  topic
                    dr.close();
                }
            }
            return reader.get();
        }

        private DataWriter<T> writer() {
            if (writer.get() == null) {
                DataWriter<T> dw = pub.createDataWriter(topic(), writerQos(pub, durability, history));
                if (!writer.compareAndSet(null, dw)) {
                    // someone else must have created the  topic
                    dw.close();
                }
            }
            return writer.get();
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

        private static DataReaderQos readerQos(Subscriber sub, Durability.Kind durability) {
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

        private DataWriterQos writerQos(Publisher pub, Durability.Kind durability) {
            return writerQos(pub, durability, 1);
        }

        private static TopicQos topicQos(DomainParticipant dp, Durability.Kind durability, ResourceLimits rlimits) {
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

        private static TopicQos topicQos(Durability.Kind durability, ResourceLimits rlimits) {
            return topicQos(VConfig.DefaultEntities.defaultDomainParticipant(), durability, rlimits);
        }
    }

    public static class Event<T> {
        private final Class<T> type;
        private final String name;
        private final Durability.Kind durability;
        private final DomainParticipant dp;
        private final Subscriber sub;
        private final Publisher pub;

        private final AtomicReference<Topic<T>> topic
                = new AtomicReference<>(null);
        private final AtomicReference<DataReader<T>> reader
                = new AtomicReference<>(null);
        private final AtomicReference<DataWriter<T>> writer
                = new AtomicReference<>(null);

        public Event(String name, Class<T> type, Durability.Kind durability, DomainParticipant dp, Subscriber sub, Publisher pub) {
            this.type = type;
            this.name = name;
            this.durability = durability;
            this.dp = dp;
            this.sub = sub;
            this.pub = pub;
        }

        public Event(String name, Class<T> type, Durability.Kind durability, DomainParticipant dp) {
            this(name, type, durability, dp, dp.createSubscriber(), dp.createPublisher());
        }

        public Event(String name, Class<T> type, Durability.Kind durability) {
            this(name, type, durability, VConfig.DefaultEntities.defaultDomainParticipant(), VConfig.DefaultEntities.defaultSub(), VConfig.DefaultEntities.defaultPub());
        }

        private Topic<T> topic() {
            if (topic.get() == null) {
                Topic t = dp.createTopic(name, type, dp.getDefaultTopicQos(), null);
                if (!topic.compareAndSet(null, t)) {
                    // someone else must have created the  topic
                    t.close();
                }
            }
            return topic.get();
        }

        private DataReader<T> reader() {
            if (reader.get() == null) {
                DataReader<T> dr = sub.createDataReader(topic(), readerQos(sub, durability));
                if (!reader.compareAndSet(null, dr)) {
                    // someone else must have created the  topic
                    dr.close();
                }
            }
            return reader.get();
        }

        private DataWriter<T> writer() {
            if (writer.get() == null) {
                DataWriter<T> dw = pub.createDataWriter(topic(), writerQos(pub, durability));
                if (!writer.compareAndSet(null, dw)) {
                    // someone else must have created the  topic
                    dw.close();
                }
            }
            return writer.get();
        }

        private static DataReaderQos readerQos(Subscriber sub, Durability.Kind durability) {
            final PolicyFactory pf = VConfig.DefaultEntities.defaultPolicyFactory();

            return sub.getDefaultDataReaderQos().withPolicies(
                    pf.Reliability().withReliable(),
                    pf.Durability().withKind(durability),
                    pf.History().withKeepAll(),
                    pf.ReaderDataLifecycle().withAutoPurgeDisposedSamplesDelay(0, TimeUnit.SECONDS)
            );
        }

        private static DataWriterQos writerQos(Publisher pub, Durability.Kind durability) {
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
