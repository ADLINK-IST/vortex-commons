package vortex.commons.util;

import org.omg.dds.core.event.DataAvailableEvent;
import org.omg.dds.core.policy.Durability;
import org.omg.dds.core.policy.ResourceLimits;
import org.omg.dds.domain.DomainParticipant;
import org.omg.dds.pub.DataWriter;
import org.omg.dds.pub.DataWriterQos;
import org.omg.dds.pub.Publisher;
import org.omg.dds.sub.*;
import org.omg.dds.topic.Topic;
import org.omg.dds.topic.TopicQos;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static vortex.commons.util.LambdaExceptionUtil.rethrowConsumer;

/**
 * Created by Vortex.
 */
public abstract class Idiom<T> {
    protected final Class<T> type;
    protected final String name;
    protected final Durability.Kind durability;
    protected final ResourceLimits rlimits;
    protected final DomainParticipant dp;
    protected final Subscriber sub;
    protected final Publisher pub;
    private final AtomicReference<Topic<T>> topic
            = new AtomicReference<>(null);
    private final AtomicReference<DataReader<T>> reader
            = new AtomicReference<>(null);
    private final AtomicReference<DataWriter<T>> writer
            = new AtomicReference<>(null);
    private final AtomicReference<DataReaderListener<T>> listener
            = new AtomicReference<>(null);
    private final List<Consumer<Sample<T>>> consumers =
            new CopyOnWriteArrayList<>();

    public Idiom(Class<T> type, DomainParticipant dp, String name, Subscriber sub, Publisher pub, Durability.Kind durability) {
        this.type = type;
        this.dp = dp;
        this.name = name;

        this.rlimits = VConfig.DefaultEntities.defaultPolicyFactory().ResourceLimits().withMaxSamples(1).withMaxInstances(1024).withMaxSamplesPerInstance(1024 * 1024);
        this.sub = sub;
        this.pub = pub;
        this.durability = durability;
    }

    public List<T> take() {
        final ArrayList<T> result = new ArrayList<>();
        reader().select().dataState(DataState.ALL_DATA.state()).take().forEachRemaining(next -> result.add(next.getData()));
        return result;
    }

    public void observe(Consumer<Sample<T>> handler) {
        consumers.add(handler);
        if(listener.get() == null) {
            DataReaderAdapter<T> adapter = new DataReaderAdapter<T>() {
                @Override
                public void onDataAvailable(DataAvailableEvent<T> status) {
                    ArrayList<Sample<T>> samples = new ArrayList<>();
                    reader().read(samples);
                    samples.forEach(s -> consumers.forEach(c -> c.accept(s)));
                }
            };
            listener.compareAndSet(null, adapter);
            reader().setListener(adapter);
        }
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

    protected abstract TopicQos topicQos(DomainParticipant dp, Durability.Kind durability, ResourceLimits rlimits);
    protected abstract DataReaderQos readerQos(Subscriber sub, Durability.Kind durability);
    protected abstract DataWriterQos writerQos(Publisher pub, Durability.Kind durability);
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
}
