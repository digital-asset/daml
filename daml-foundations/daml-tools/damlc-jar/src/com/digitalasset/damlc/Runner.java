// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.damlc;

import java.io.*;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.*;

public class Runner {

    private static String name = "da-hs-damlc-app";

    // Main entry-point. Useful when running the jar directly.
    public static void main(String[] args) {
        try {
            int result = run(args);
            if (result != 0) {
                System.err.println("damlc process stopped with exit status " + result);
                System.exit(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    private static File extractedDamlc = null;

    public static int run(String[] args) throws IOException, URISyntaxException, InterruptedException {
        return runAsync(args).waitFor();
    }

    public static Process runAsync(String[] args)  throws IOException, URISyntaxException, InterruptedException {
        File damlc = extract();
        List<String> command = new ArrayList<String>();
        command.add(damlc.getAbsolutePath());
        command.addAll(Arrays.asList(args));

        ProcessBuilder pb = new ProcessBuilder(command);
        Process proc = pb.start();
        new Thread(new CopyThread(proc.getInputStream(), System.out)).start();
        new Thread(new CopyThread(proc.getErrorStream(), System.err)).start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                proc.destroy();
            }
        });
        return proc;
    }

    // Extract the archive to a temporary directory
    // and return the binary path
    public static File extract() throws IOException, URISyntaxException {
        synchronized (Runner.class) {
            if (extractedDamlc == null) {
                extractedDamlc = extractInternal();
            }
            return extractedDamlc;
        }
    }

    private static File extractInternal() throws IOException, URISyntaxException {
        File tempDir = File.createTempFile("damlc-runner", "", null);
        tempDir.delete();
        tempDir.mkdirs();
        tempDir.deleteOnExit();

        // Extract dynamic libraries
        Enumeration<URL> en = Runner.class.getClassLoader().getResources(name);
        URL url = en.nextElement();
        JarURLConnection urlcon = (JarURLConnection) (url.openConnection());
        JarFile jar = urlcon.getJarFile();
        Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
            String entry = entries.nextElement().getName();
            if (entry.startsWith(name) && !entry.endsWith("/")) {
              extractResource(entry, tempDir);
            }
        }

        // Return the path to the damlc binary
        File damlcFile = tempDir.toPath().resolve(name + "/" + name).toFile();
        return damlcFile;
    }

    private static void extractResource(String resource, File outputDir) throws IOException {
        File dst = new File(outputDir, resource);
        File parent = dst.getParentFile();
        parent.mkdirs();
        parent.deleteOnExit();
        OutputStream os = new FileOutputStream(dst);
        InputStream is = Runner.class.getResourceAsStream("/" + resource);
        dst.setExecutable(true);
        dst.deleteOnExit();
        try {
            int n = 0;
            byte[] buf = new byte[4096];
            while ((n = is.read(buf)) > 0) os.write(buf, 0, n);
        }
        finally {
            is.close();
            os.close();
        }
    }


    static class CopyThread implements Runnable
    {
        private InputStream _is;
        private OutputStream _os;
        public CopyThread(InputStream is, OutputStream os) {
            _is = is; _os = os;
        }

        public void run() {
            try {
                int n = 0;
                byte[] buf = new byte[4096];
                while ((n = _is.read(buf)) > 0) _os.write(buf, 0, n);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}


