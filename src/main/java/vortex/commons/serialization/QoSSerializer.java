package vortex.commons.serialization;

import com.google.gson.*;
import org.omg.dds.core.Duration;
import org.omg.dds.core.policy.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Created by Vortex.
 */
public class QoSSerializer implements Serializer<QosPolicy[]>  {

    public static final int DDS_INVALID_QOS_POLICY_ID = 0;
    public static final int DDS_USERDATA_QOS_POLICY_ID = 1;
    public static final int DDS_DURABILITY_QOS_POLICY_ID = 2;
    public static final int DDS_PRESENTATION_QOS_POLICY_ID = 3;
    public static final int DDS_DEADLINE_QOS_POLICY_ID = 4;
    public static final int DDS_LATENCYBUDGET_QOS_POLICY_ID = 5;
    public static final int DDS_OWNERSHIP_QOS_POLICY_ID = 6;
    public static final int DDS_OWNERSHIPSTRENGTH_QOS_POLICY_ID = 7;
    public static final int DDS_LIVELINESS_QOS_POLICY_ID = 8;
    public static final int DDS_TIMEBASEDFILTER_QOS_POLICY_ID = 9;
    public static final int DDS_PARTITION_QOS_POLICY_ID = 10;
    public static final int DDS_RELIABILITY_QOS_POLICY_ID = 11;
    public static final int DDS_DESTINATIONORDER_QOS_POLICY_ID = 12;
    public static final int DDS_HISTORY_QOS_POLICY_ID = 13;
    public static final int DDS_RESOURCELIMITS_QOS_POLICY_ID = 14;
    public static final int DDS_ENTITYFACTORY_QOS_POLICY_ID = 15;
    public static final int DDS_WRITERDATALIFECYCLE_QOS_POLICY_ID = 16;
    public static final int DDS_READERDATALIFECYCLE_QOS_POLICY_ID = 17;
    public static final int DDS_TOPICDATA_QOS_POLICY_ID = 18;
    public static final int DDS_GROUPDATA_QOS_POLICY_ID = 19;
    public static final int DDS_TRANSPORTPRIORITY_QOS_POLICY_ID = 20;
    public static final int DDS_LIFESPAN_QOS_POLICY_ID = 21;
    public static final int DDS_DURABILITYSERVICE_QOS_POLICY_ID = 22;

    /**
       QoS Kinds
    **/
    //Durability
    public static final int DDS_DURABILITY_VOLATILE = 0;
    public static final int DDS_DURABILITY_TRANSIENT_LOCAL = 1;
    public static final int DDS_DURABILITY_TRANSIENT = 2;
    public static final int DDS_DURABILITY_PERSISTENT = 3;

    // History
    public static final int DDS_HISTORY_KEEP_LAST = 0;
    public static final int DDS_HISTORY_KEEP_ALL = 1;

    // Ownership
    public static final int DDS_OWNERSHIP_SHARED = 0;
    public static final int DDS_OWNERSHIP_EXCLUSIVE = 1;

    //Reliability
    public static final int DDS_RELIABILITY_BEST_EFFORT = 0;
    public static final int DDS_RELIABILITY_RELIABLE = 1;

    // Dest Order
    public static final int DDS_DESTINATIONORDER_BY_RECEPTION_TIMESTAMP = 0;
    public static final int DDS_DESTINATIONORDER_BY_SOURCE_TIMESTAMP = 1;

    public static final int DDS_LIVELINESS_AUTOMATIC = 0;
    public static final int DDS_LIVELINESS_MANUAL_BY_PARTICIPANT = 1;
    public static final int DDS_LIVELINESS_MANUAL_BY_TOPIC = 2;

    private final PolicyFactory pf;

    public QoSSerializer(PolicyFactory pf) {
        this.pf = Objects.requireNonNull(pf);
    }

    @Override
    public Serializable toSerializable(QosPolicy[] policies) {
        JsonArray ja = new JsonArray();
        for(QosPolicy p : policies) {
            if(p instanceof UserData) {

            } else if(p instanceof Durability) {
                Durability d =  (Durability) p;
                JsonObject jobj = new JsonObject();
                jobj.addProperty("id", DDS_DURABILITY_QOS_POLICY_ID);
                int k = -1;
                switch (d.getKind()) {
                    case VOLATILE:
                        k = DDS_DURABILITY_VOLATILE;
                        break;
                    case TRANSIENT:
                        k = DDS_DURABILITY_TRANSIENT;
                        break;
                    case TRANSIENT_LOCAL:
                        k = DDS_DURABILITY_TRANSIENT_LOCAL;
                        break;
                    case PERSISTENT:
                        k = DDS_DURABILITY_PERSISTENT;
                        break;
                }
                jobj.addProperty("k", k);
                ja.add(jobj);
            } else if(p instanceof Presentation) {

            } else if(p instanceof Deadline) {

            } else if(p instanceof LatencyBudget) {

            } else if(p instanceof Ownership) {

            } else if(p instanceof OwnershipStrength) {

            } else if(p instanceof Liveliness) {

            } else if(p instanceof TimeBasedFilter) {

            } else if(p instanceof Partition) {

            } else if(p instanceof Reliability) {
                Reliability r = (Reliability) p;
                JsonObject jobj = new JsonObject();
                jobj.addProperty("id", DDS_RELIABILITY_QOS_POLICY_ID);
                switch (r.getKind()) {
                    case RELIABLE:
                        jobj.addProperty("k", DDS_RELIABILITY_RELIABLE);
                        jobj.add("mbt", serialize(r.getMaxBlockingTime()));
                        break;
                    case BEST_EFFORT:
                        jobj.addProperty("k", DDS_RELIABILITY_BEST_EFFORT);
                        break;
                }
                ja.add(jobj);
            } else if(p instanceof DestinationOrder) {

            } else if(p instanceof History) {

            } else if(p instanceof ResourceLimits) {

            } else if(p instanceof EntityFactory) {

            } else if(p instanceof WriterDataLifecycle) {

            } else if(p instanceof ReaderDataLifecycle) {

            } else if(p instanceof TopicData) {

            } else if(p instanceof GroupData) {

            } else if(p instanceof TransportPriority) {

            } else if(p instanceof Lifespan) {

            } else if(p instanceof DurabilityService) {

            } else {
                throw new IllegalArgumentException("Unrecognized policy type");
            }
        }
        return ja.toString();
    }

    private JsonObject serialize(Duration d) {
        JsonObject j = new JsonObject();
        j.addProperty("i", d.isInfinite());
        j.addProperty("z", d.isZero());
        if(!d.isInfinite() && !d.isZero()){
            j.addProperty("d", d.getDuration(TimeUnit.MILLISECONDS));
        }
        return j;
    }

    @Override
    public boolean canDeserialize(Serializable o) {
        return o instanceof String;
    }

    @Override
    public QosPolicy[] fromSerializable(Serializable o) {
        ArrayList<QosPolicy> policies = new ArrayList<>();
        if(o instanceof String) {
            JsonParser parser = new JsonParser();

            JsonElement je = parser.parse((String) o);

            if(je.isJsonArray()) {
                JsonArray ja = je.getAsJsonArray();

                ja.forEach(element -> {
                    if(element instanceof JsonObject) {
                        JsonObject jobj = element.getAsJsonObject();
                        Optional<QosPolicy> qosPolicy = toPolicy(jobj);
                        qosPolicy.ifPresent(p -> policies.add(p));
                    }
                });
            } else {
                throw new IllegalArgumentException("Needs to be a Json Array.");
            }
        } else {
            throw new IllegalArgumentException("Needs to be a stringified Json Array");
        }
        return policies.toArray(new QosPolicy[0]);
    }

    private Optional<QosPolicy> toPolicy(JsonObject jobj) {
        Optional<QosPolicy> result = Optional.empty();

        if(jobj.has("id")){
            int id = jobj.getAsJsonPrimitive("id").getAsInt();
            switch (id) {
                case DDS_INVALID_QOS_POLICY_ID:
                    break;
                case DDS_USERDATA_QOS_POLICY_ID:
                    break;
                case DDS_DURABILITY_QOS_POLICY_ID:
                    result = toDurability(jobj);
                    break;
                case DDS_PRESENTATION_QOS_POLICY_ID:
                    break;
                case DDS_DEADLINE_QOS_POLICY_ID:
                    break;
                case DDS_LATENCYBUDGET_QOS_POLICY_ID:
                    break;
                case DDS_OWNERSHIP_QOS_POLICY_ID:
                    break;
                case DDS_OWNERSHIPSTRENGTH_QOS_POLICY_ID:
                    break;
                case DDS_LIVELINESS_QOS_POLICY_ID:
                    break;
                case DDS_TIMEBASEDFILTER_QOS_POLICY_ID:
                    break;
                case DDS_PARTITION_QOS_POLICY_ID:
                    break;
                case DDS_RELIABILITY_QOS_POLICY_ID:
                    result = toReliability(jobj);
                    break;
                case DDS_DESTINATIONORDER_QOS_POLICY_ID:
                    break;
                case DDS_HISTORY_QOS_POLICY_ID:
                    break;
                case DDS_RESOURCELIMITS_QOS_POLICY_ID:
                    break;
                case DDS_ENTITYFACTORY_QOS_POLICY_ID:
                    break;
                case DDS_WRITERDATALIFECYCLE_QOS_POLICY_ID:
                    break;
                case DDS_READERDATALIFECYCLE_QOS_POLICY_ID:
                    break;
                case DDS_TOPICDATA_QOS_POLICY_ID:
                    break;
                case DDS_GROUPDATA_QOS_POLICY_ID:
                    break;
                case DDS_TRANSPORTPRIORITY_QOS_POLICY_ID:
                    break;
                case DDS_LIFESPAN_QOS_POLICY_ID:
                    break;
                case DDS_DURABILITYSERVICE_QOS_POLICY_ID:
                    break;
            }
        }
        return result;
    }

    private Optional<QosPolicy> toDurability(JsonObject jobj) {
        Optional<QosPolicy> result = Optional.empty();
        int k = jobj.getAsJsonPrimitive("k").getAsInt();
        switch (k) {
            case DDS_DURABILITY_VOLATILE:
                result = Optional.of(pf.Durability().withVolatile());
                break;
            case DDS_DURABILITY_TRANSIENT_LOCAL:
                result = Optional.of(pf.Durability().withTransientLocal());
                break;
            case DDS_DURABILITY_TRANSIENT:
                result = Optional.of(pf.Durability().withTransient());
                break;
            case DDS_DURABILITY_PERSISTENT:
                result = Optional.of(pf.Durability().withPersistent());
                break;
        }
        return result;
    }

    private Optional<QosPolicy> toReliability(JsonObject jobj) {
        Optional<QosPolicy> result = Optional.empty();
        int k = jobj.getAsJsonPrimitive("k").getAsInt();
        switch (k) {
            case DDS_RELIABILITY_BEST_EFFORT:
                result = Optional.of(pf.Reliability().withBestEffort());
                break;
            case DDS_RELIABILITY_RELIABLE:
                Reliability reliability = pf.Reliability().withReliable();

                if(jobj.has("mbt")) {
                    JsonObject mbt = jobj.getAsJsonObject("mbt");
                    boolean i = mbt.getAsJsonPrimitive("i").getAsBoolean();
                    boolean z = mbt.getAsJsonPrimitive("z").getAsBoolean();
                    if(i) {
                        reliability = reliability.withMaxBlockingTime(Duration.infiniteDuration(pf.getEnvironment()));
                    } else if(z) {
                        reliability = reliability.withMaxBlockingTime(Duration.zeroDuration(pf.getEnvironment()));
                    } else {
                        long d = mbt.getAsJsonPrimitive("d").getAsLong();
                        reliability = reliability.withMaxBlockingTime(d, TimeUnit.MILLISECONDS);
                    }
                }
                result = Optional.of(reliability);
                break;
        }
        return result;
    }
}
