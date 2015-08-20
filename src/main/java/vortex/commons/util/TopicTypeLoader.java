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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class TopicTypeLoader {
    private static Logger LOG = LoggerFactory.getLogger(TopicTypeLoader.class);

    private TopicTypeLoader() {
    }

    public static void loadTypesFromProperties() {
        final String[] classpaths = System.getProperty("vortex.types.classpath", "").split(":");
        final List<File> files = new ArrayList<>();
        for (String cp : classpaths) {
            if (cp != null && !cp.isEmpty()) {
                final File f = new File(cp);
                if (f.exists()) {
                    files.add(f);
                } else {
                    LOG.warn("Path {} does not exist !", cp);
                }
            }
        }

    }

    private void loadClasspaths(final List<File> classpaths) {
        Method addURLMethod;

        try {
            addURLMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        } catch (NoSuchMethodException e) {
            LOG.error("Unable to access the URLClassLoader#addURL. No class paths will be loaded.", e);
            addURLMethod = null;
        }

        final Method method = addURLMethod;
        if (method != null) {
            method.setAccessible(true);

            classpaths.forEach(cp -> {
                try {
                    final URL url = cp.toURI().toURL();
                    method.invoke(this.getClass().getClassLoader(), url);
                    LOG.info("{} loaded.", url.toString());
                } catch (MalformedURLException e) {
                    LOG.warn("Error loading classpath {}.", cp.getAbsolutePath());
                    LOG.warn("Error loading classpath. [exception]", e);
                } catch (InvocationTargetException e) {
                    LOG.warn("Error loading classpath {}.", cp.getAbsolutePath());
                    LOG.warn("Error loading classpath. [exception]", e);
                } catch (IllegalAccessException e) {
                    LOG.warn("Error loading classpath {}.", cp.getAbsolutePath());
                    LOG.warn("Error loading classpath. [exception]", e);
                }
            });
        }
    }

}
