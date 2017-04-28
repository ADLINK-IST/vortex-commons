package vortex.commons.serialization;

import java.io.Serializable;

/**
 * Created by Vortex.
 */
public interface Serializer<T> {

    Serializable toSerializable(T objectToSerialize);

    //to build a factory/service around it
    boolean canDeserialize(Serializable serializedObject);

    T fromSerializable(Serializable serializedObject);
}
