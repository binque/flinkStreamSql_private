package com.cj.flink.sql.classloader;

import com.cj.flink.sql.util.PluginUtil;
import com.cj.flink.sql.util.ReflectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClassLoaderManager {
    private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderManager.class);

    //插件路径 ： 加载器
    //插件的url : 加载器
    private static Map<String, DtClassLoader> pluginClassLoader = new ConcurrentHashMap<>();

    public static <R> R newInstance(String pluginJarPath, ClassLoaderSupplier<R> supplier) throws Exception {
        ClassLoader classLoader = retrieveClassLoad(pluginJarPath);
        return ClassLoaderSupplierCallBack.callbackAndReset(supplier, classLoader);
    }

    public static <R> R newInstance(List<URL> jarUrls, ClassLoaderSupplier<R> supplier) throws Exception {
        ClassLoader classLoader = retrieveClassLoad(jarUrls);
        return ClassLoaderSupplierCallBack.callbackAndReset(supplier, classLoader);
    }


    private static DtClassLoader retrieveClassLoad(String pluginJarPath) {
        return pluginClassLoader.computeIfAbsent(pluginJarPath, k -> {
            try {
                URL[] urls = PluginUtil.getPluginJarUrls(pluginJarPath);
                ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
                DtClassLoader classLoader = new DtClassLoader(urls, parentClassLoader);
                LOG.info("pluginJarPath:{} create ClassLoad successful...", pluginJarPath);
                return classLoader;
            } catch (Throwable e) {
                LOG.error("retrieve ClassLoad happens error:{}", e);
                throw new RuntimeException("retrieve ClassLoad happens error");
            }
        });
    }

    private static DtClassLoader retrieveClassLoad(List<URL> jarUrls) {
        jarUrls.sort(Comparator.comparing(URL::toString));
        String jarUrlkey = StringUtils.join(jarUrls, "_");
        return pluginClassLoader.computeIfAbsent(jarUrlkey, k -> {
            try {
                URL[] urls = jarUrls.toArray(new URL[jarUrls.size()]);
                ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
                DtClassLoader classLoader = new DtClassLoader(urls, parentClassLoader);
                LOG.info("jarUrl:{} create ClassLoad successful...", jarUrlkey);
                return classLoader;
            } catch (Throwable e) {
                LOG.error("retrieve ClassLoad happens error:{}", e);
                throw new RuntimeException("retrieve ClassLoad happens error");
            }
        });
    }

    public static List<URL> getClassPath() {
        List<URL> classPaths = new ArrayList<>();
        for (Map.Entry<String, DtClassLoader> entry : pluginClassLoader.entrySet()) {
            classPaths.addAll(Arrays.asList(entry.getValue().getURLs()));
        }
        return classPaths;
    }

    public static URLClassLoader loadExtraJar(List<URL> jarUrlList, URLClassLoader classLoader)
            throws  IllegalAccessException, InvocationTargetException {
        for(URL url : jarUrlList){
            if(url.toString().endsWith(".jar")){
                urlClassLoaderAddUrl(classLoader, url);
            }
        }
        return classLoader;
    }

    private static void urlClassLoaderAddUrl(URLClassLoader classLoader, URL url) throws InvocationTargetException, IllegalAccessException {
        Method method = ReflectionUtils.getDeclaredMethod(classLoader, "addURL", URL.class);

        if (method == null) {
            throw new RuntimeException("can't not find declared method addURL, curr classLoader is " + classLoader.getClass());
        }
        method.setAccessible(true);
        method.invoke(classLoader, url);
    }

}
