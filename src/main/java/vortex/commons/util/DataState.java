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

import org.omg.dds.sub.InstanceState;
import org.omg.dds.sub.SampleState;
import org.omg.dds.sub.Subscriber;
import org.omg.dds.sub.ViewState;

public enum DataState {
    ALL_SAMPLES(VConfig.DefaultEntities.defaultSub().createDataState().withAnyInstanceState().withAnySampleState().withAnyViewState()),
    ALL_DATA(VConfig.DefaultEntities.defaultSub().createDataState().withAnySampleState().withAnyViewState().with(InstanceState.ALIVE)),
    NEW_DATA(VConfig.DefaultEntities.defaultSub().createDataState().withAnyViewState().with(InstanceState.ALIVE).with(SampleState.NOT_READ)),
    OLD_DATA(VConfig.DefaultEntities.defaultSub().createDataState().withAnyViewState().with(InstanceState.ALIVE).with(SampleState.READ)),
    NEW_INSTANCES(VConfig.DefaultEntities.defaultSub().createDataState().with(ViewState.NEW).withAnySampleState().withAnyInstanceState()),
    NOT_ALIVE_INSTANCES(VConfig.DefaultEntities.defaultSub().createDataState().with(InstanceState.NOT_ALIVE_NO_WRITERS).withAnySampleState().withAnyViewState()),
    DISPOSED_INSTANCES(VConfig.DefaultEntities.defaultSub().createDataState().with(InstanceState.NOT_ALIVE_DISPOSED).withAnySampleState().withAnyViewState());

    private final Subscriber.DataState state;

    DataState(Subscriber.DataState state) {
        this.state = state;
    }

    public Subscriber.DataState state() {
        return state;
    };
}
