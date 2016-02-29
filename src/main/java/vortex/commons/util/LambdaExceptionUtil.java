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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

final class LambdaExceptionUtil {

    @FunctionalInterface
    public interface Consumer_WithExceptions<T, E extends Exception> {
        void accept(T t) throws E;
    }

    @FunctionalInterface
    public interface BiConsumer_WithExceptions<T, U, E extends Exception> {
        void accept(T t, U u) throws E;
    }

    @FunctionalInterface
    public interface Function_WithExceptions<T, R, E extends Exception> {
        R apply(T t) throws E;
    }

    @FunctionalInterface
    public interface Supplier_WithExceptions<T, E extends Exception> {
        T get() throws E;
    }

    @FunctionalInterface
    public interface Runnable_WithExceptions<E extends Exception> {
        void run() throws E;
    }

    public static <T, E extends Exception> Consumer<T> rethrowConsumer(Consumer_WithExceptions<T, E> consumer) {
        return t -> {
            try { consumer.accept(t); }
            catch(Exception e) { throwAsUnchecked(e); }
        };
    }

    public static <T, U, E extends Exception> BiConsumer<T, U> rethrowBiConsumer(BiConsumer_WithExceptions<T,U,E> biConsumer) {
        return (t,u) -> {
            try {
                biConsumer.accept(t, u);
            } catch (Exception e) {
                throwAsUnchecked(e);
            }
        };
    }

    /** .map(rethrowFunction(name -> Class.forName(name))) or .map(rethrowFunction(Class::forName)) */
    public static <T, R, E extends Exception> Function<T, R> rethrowFunction(Function_WithExceptions<T, R, E> function) {
        return t -> {
            try { return function.apply(t); }
            catch (Exception exception) { throwAsUnchecked(exception); return null; }
        };
    }

    /** rethrowSupplier(() -> new StringJoiner(new String(new byte[]{77, 97, 114, 107}, "UTF-8"))), */
    public static <T, E extends Exception> Supplier<T> rethrowSupplier(Supplier_WithExceptions<T, E> function) {
        return () -> {
            try { return function.get(); }
            catch (Exception exception) { throwAsUnchecked(exception); return null; }
        };
    }

    @SuppressWarnings ("unchecked")
    private static <E extends Throwable> void throwAsUnchecked(Exception exception) throws E { throw (E)exception; }
}
