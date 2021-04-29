package org.jgroups.upgrade_server;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Bela Ban
 * @since  1.0.0
 */
public class UpgradeServer {
    protected Server server;
    private File configFile;
    private WatchService configWatch;

    public UpgradeServer(String config) {

        if (config != null && new File(config).isFile()) {
            configFile = new File(config);
            try {
                Path configPath = Paths.get(configFile.getParentFile().getAbsolutePath());
                configWatch = FileSystems.getDefault().newWatchService();
                configPath.register(configWatch, StandardWatchEventKinds.ENTRY_MODIFY);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("no configuration file specified");
        }
    }

    public void start(int port) throws Exception {
        final UpgradeService service = new UpgradeService();

        server = ServerBuilder.forPort(port)
          .addService(service)
          .build()
          .start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("sending deactivate command to all nodes");
            service.terminate();
            System.out.println("awaiting termination of connections to remote cluster nodes...");
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                // ignore
            }
            server.shutdown();
            System.out.println("server was shut down");
        }));

        if (configFile != null) {
            Thread watchThread = new Thread(new PropertiesWatcher(service));
            watchThread.setDaemon(true);
            watchThread.start();
        }

        System.out.printf("-- UpgradeServer listening on %d\n", server.getPort());
        server.awaitTermination();

    }

    public void stop() throws InterruptedException {
        server.awaitTermination();
    }

    protected void blockUntilShutdown() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        int port=50051;
        String config = null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-p") || args[i].equals("-port")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-c") || args[i].equals("-config")) {
                config=args[++i];
                continue;
            }
            help();
            return;
        }

        UpgradeServer srv=new UpgradeServer(config);
        srv.start(port);
        srv.blockUntilShutdown();
    }

    protected static void help() {
        System.out.println("UpgradeServer [-config <config file>] [-port <server port>]");
    }


    private class PropertiesWatcher implements Runnable {
        private UpgradeService serviceRef;

        public PropertiesWatcher(UpgradeService svc) {
            serviceRef = svc;
        }

        public void run() {
            try {
                WatchKey key;
                while ((key = configWatch.take()) != null) {
                    Path path = (Path) key.watchable();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        //process
                        File file = path.resolve((Path) event.context()).toFile();
                        if (file.getAbsolutePath().equals(configFile.getAbsolutePath())) {
                            System.out.println("Config file changed!");
                            try (InputStream s = new FileInputStream(file)) {
                                Properties p  = new Properties();
                                p.load(s);
                                String active = (String) p.get("active");
                                if (active != null) {
                                    System.out.printf("Sending command activate %s\n", Boolean.valueOf(active));

                                    if (Boolean.valueOf(active)) {
                                        serviceRef.activate();
                                    } else {
                                        serviceRef.deactivate();
                                    }
                                }
                            }
                        }
                    }
                    key.reset();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
