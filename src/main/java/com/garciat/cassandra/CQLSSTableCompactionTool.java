package com.garciat.cassandra;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.CompactionStats;
import org.apache.commons.codec.Charsets;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;

public class CQLSSTableCompactionTool {

    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CQLSSTableCompactionTool.class);

    private static final int DEFAULT_JMX_PORT = 7199;

    private static final int COMPACTION_MONITOR_INTERVAL = 5000;

    private static final String CREATE_KEYSPACE_CQL =
            "CREATE KEYSPACE %s " +
                    "WITH REPLICATION = {" +
                    "'class' : 'org.apache.cassandra.locator.SimpleStrategy'," +
                    "'replication_factor': '1' }" +
                    "AND DURABLE_WRITES = true;";

    private final Options options;

    private CQLSSTableCompactionTool(Options options) {
        this.options = options;
    }

    private void run() throws Exception {
        configureLoggers();

        try {
            File storageDir = setUpDirectoryStructure();

            setUpDaemonProperties(storageDir);

            LOGGER.info("Starting Cassandra...");

            startDaemon();

            LOGGER.info("Creating schema...");

            setUpSchema();

            LOGGER.info("Compacting...");

            compactAndStop();

            LOGGER.info("DONE");
        } catch (ToolException e) {
            System.err.println(e.getMessage());
            if (e.getCause() != null) {
                e.getCause().printStackTrace();
            }
            System.exit(1);
        }
    }

    private void configureLoggers() {
        Logger cassandraLogger = (Logger) LoggerFactory.getLogger("org.apache.cassandra");
        cassandraLogger.setLevel(Level.ERROR);

        Logger nettyLogger = (Logger) LoggerFactory.getLogger("io.netty");
        nettyLogger.setLevel(Level.ERROR);

        Logger datastaxDriverLogger = (Logger) LoggerFactory.getLogger("com.datastax.driver");
        datastaxDriverLogger.setLevel(Level.ERROR);

        // TODO This one won't shut up.
        org.apache.log4j.Logger.getLogger("Sigar").setLevel(org.apache.log4j.Level.ERROR);
    }

    private File setUpDirectoryStructure() {
        File storageDir = Files.createTempDir();

        Path keyspaceStorageDir = Paths.get(storageDir.getAbsolutePath(), "data", options.keyspaceName);

        if (!keyspaceStorageDir.toFile().mkdirs()) {
            throw new ToolException("Could not create path: " + keyspaceStorageDir);
        }

        Path tableStorageDir = keyspaceStorageDir.resolve(options.tableName);

        try {
            java.nio.file.Files.createSymbolicLink(tableStorageDir, options.inputDir.toPath());
        } catch (IOException e) {
            throw new ToolException("Could not symlink directory at: " + tableStorageDir, e);
        }

        return storageDir;
    }

    private void startDaemon() {
        CassandraDaemon daemon = new CassandraDaemon();

        daemon.applyConfig();

        try {
            daemon.init(null);
        } catch (IOException e) {
            throw new ToolException("Could not start Cassandra.", e);
        }

        daemon.start();
    }

    private void setUpSchema() {
        try (Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
             Session session = cluster.connect()) {
            session.execute(format(CREATE_KEYSPACE_CQL, options.keyspaceName));

            session.execute(options.tableDdl);
        }
    }

    private void compactAndStop() throws IOException, ExecutionException, InterruptedException {
        NodeProbe probe = new NodeProbe("localhost", DEFAULT_JMX_PORT);

        Thread compactionMonitor = startCompactionMonitor(probe);

        probe.forceKeyspaceCompaction(false, options.keyspaceName, options.tableName);

        compactionMonitor.interrupt();

        probe.stopCassandraDaemon();
    }

    private Thread startCompactionMonitor(NodeProbe probe) {
        Thread compactionMonitor = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    CompactionStats.reportCompactionTable(probe.getCompactionManagerProxy().getCompactions(), probe.getCompactionThroughput(), true);

                    Thread.sleep(COMPACTION_MONITOR_INTERVAL);
                }
            } catch (Exception e) {
                // don't care
            }
        });

        compactionMonitor.start();

        return compactionMonitor;
    }

    private void setUpDaemonProperties(File storageDir) {
        File configFile = loadConfigFileIntoDir(storageDir);

        System.setProperty("cassandra.config", configFile.toURI().toString());
        System.setProperty("cassandra.storagedir", storageDir.getAbsolutePath());
        System.setProperty("cassandra.jmx.local.port", String.valueOf(DEFAULT_JMX_PORT));
    }

    private File loadConfigFileIntoDir(File storageDir) {
        File configFile = new File(storageDir, "cassandra.yaml");

        try {
            String configContents = Resources.toString(Resources.getResource("cassandra.yaml"), Charsets.UTF_8);

            Files.write(configContents, configFile, Charsets.UTF_8);
        } catch (IOException e) {
            throw new ToolException("Could not load cassandra.yaml from resources.");
        }

        return configFile;
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);

        new CQLSSTableCompactionTool(options).run();
    }

    private static class Options {

        final String keyspaceName;
        final String tableName;
        final File inputDir;
        final String tableDdl;

        private Options(String keyspaceName, String tableName, File inputDir, String tableDdl) {
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.inputDir = inputDir;
            this.tableDdl = tableDdl;
        }

        static Options parse(String[] args) {
            if (args.length != 4) {
                throw new ToolException(getUsage());
            }

            String keyspaceName = args[0];
            String tableName = args[1];
            File inputDir = new File(args[2]);

            if (!inputDir.exists() || !inputDir.isDirectory()) {
                throw new ToolException("Input path does not exist or is not a directory.");
            }

            String tableDdl;
            try {
                tableDdl = Files.toString(new File(args[3]), Charsets.UTF_8);
            } catch (Exception e) {
                throw new ToolException("Could not read table DDL.");
            }

            return new Options(keyspaceName, tableName, inputDir, tableDdl);
        }

        static String getUsage() {
            return "usage: {prog} KEYSPACE TABLE INPUT_DIR TABLE_DDL_FILE\n" +
                    "Properties:\n" +
                    "  java.io.tmpdir - Set the temp dir base";
        }
    }

    private static class ToolException extends RuntimeException {

        ToolException(String message) {
            super(message);
        }

        ToolException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
