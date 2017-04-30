package com.garciat.cassandra;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.CompactionStats;
import org.apache.commons.codec.Charsets;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;

public class CQLSSTableCompactionTool {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CQLSSTableCompactionTool.class);

    private static final int DEFAULT_JMX_PORT = 7199;

    private static final int COMPACTION_MONITOR_INTERVAL = 5000;

    private static final String CREATE_KEYSPACE_CQL =
            "CREATE KEYSPACE %s " +
                    "WITH REPLICATION = {" +
                    "'class' : 'org.apache.cassandra.locator.SimpleStrategy'," +
                    "'replication_factor': '1' }" +
                    "AND DURABLE_WRITES = true;";

    private static final String GET_TABLE_ID_CQL =
            "SELECT id FROM system_schema.tables " +
                    "WHERE keyspace_name = ? " +
                    "AND table_name = ?";

    private final Options options;

    private CQLSSTableCompactionTool(Options options) {
        this.options = options;
    }

    private void run() throws Exception {
        if (System.getProperty("tool.verbose") == null) {
            configureLoggers();
        }

        try {
            File storageDir = createStorageDir();

            setUpDaemonProperties(storageDir);

            LOGGER.info("Starting Cassandra (you can ignore the ugly Sigar exception)");

            startDaemon();

            LOGGER.info("Creating schema");

            UUID tableId = setUpSchema();

            LOGGER.debug("created table '{}' with id={}", options.tableName, tableId);

            LOGGER.info("Setting up table data");

            Path stableStoragePath = setUpTableData(storageDir, tableId);

            LOGGER.info("Compacting");

            compactAndStop();

            LOGGER.info("Cleaning up");

            // TODO: Some Cassandra threads expect the tree to be alive -- we can't delete it just yet.
            // cleanUp(storageDir, stableStoragePath);

            LOGGER.info("DONE");

            System.exit(0);

        } catch (ToolException e) {
            System.err.println(e.getMessage());
            if (e.getCause() != null) {
                e.getCause().printStackTrace();
            }
            System.exit(1);
        }
    }

    private void cleanUp(File storageDir, Path stableStoragePath) throws IOException {
        java.nio.file.Files.delete(stableStoragePath);

        FileUtils.deleteRecursive(storageDir);
    }

    private File createStorageDir() {
        return Files.createTempDir();
    }

    private void setUpDaemonProperties(File storageDir) {
        File configFile = loadConfigFileIntoDir(storageDir);

        System.setProperty("cassandra.config", configFile.toURI().toString());
        System.setProperty("cassandra.storagedir", storageDir.getAbsolutePath());
        System.setProperty("cassandra.jmx.local.port", String.valueOf(DEFAULT_JMX_PORT));
    }

    private void startDaemon() {
        CassandraDaemon daemon = new CassandraDaemon(true);

        daemon.applyConfig();

        try {
            daemon.init(null);
        } catch (IOException e) {
            throw new ToolException("Could not start Cassandra.", e);
        }

        daemon.start();
    }

    private UUID setUpSchema() {
        try (Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
             Session session = cluster.connect()) {
            LOGGER.debug("creating keyspace");
            session.execute(format(CREATE_KEYSPACE_CQL, options.keyspaceName));

            LOGGER.debug("creating table");
            session.execute(options.tableDdl);

            LOGGER.debug("getting table id");
            return session.execute(GET_TABLE_ID_CQL, options.keyspaceName, options.tableName).one().getUUID("id");
        }
    }

    private Path setUpTableData(File storageDir, UUID tableId) throws IOException {
        String tableIdSuffix = tableId.toString().replaceAll("-", "");

        String tableDirName = options.tableName + "-" + tableIdSuffix;

        Path tableStoragePath = Paths.get(storageDir.getAbsolutePath(), "data", options.keyspaceName, tableDirName);

        if (!tableStoragePath.toFile().exists()) {
            throw new IllegalStateException("Table storage directory does not exist: " + tableStoragePath);
        }

        LOGGER.debug("deleting existing table storage dir: " + tableStoragePath);

        FileUtils.deleteRecursive(tableStoragePath.toFile());

        LOGGER.debug("creating symlink to input data");

        try {
            java.nio.file.Files.createSymbolicLink(tableStoragePath, options.inputDir.toPath());
        } catch (IOException e) {
            throw new ToolException("Could not symlink directory at: " + tableStoragePath, e);
        }

        LOGGER.debug("refreshing SSTables");

        try (NodeProbe probe = connectProbe()) {
            probe.loadNewSSTables(options.keyspaceName, options.tableName);
        }

        return tableStoragePath;
    }

    private void compactAndStop() throws IOException, ExecutionException, InterruptedException {
        try (NodeProbe probe = connectProbe()) {
            Thread compactionMonitor = startCompactionMonitor(probe);

            LOGGER.debug("starting compaction");

            probe.forceKeyspaceCompaction(false, options.keyspaceName, options.tableName);

            LOGGER.debug("compaction completed");

            compactionMonitor.interrupt();

            LOGGER.debug("stopping daemon");

            probe.stopCassandraDaemon();
        }
    }

    private Thread startCompactionMonitor(NodeProbe probe) {
        Thread compactionMonitor = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    CompactionStats.reportCompactionTable(
                            probe.getCompactionManagerProxy().getCompactions(),
                            probe.getCompactionThroughput(), true);

                    Thread.sleep(COMPACTION_MONITOR_INTERVAL);
                }
            } catch (Exception e) {
                // don't care
            }
        });

        compactionMonitor.start();

        return compactionMonitor;
    }

    private NodeProbe connectProbe() throws IOException {
        return new NodeProbe("localhost", DEFAULT_JMX_PORT);
    }

    private File loadConfigFileIntoDir(File storageDir) {
        LOGGER.debug("loading cassandra.yaml into: " + storageDir);

        File configFile = new File(storageDir, "cassandra.yaml");

        try {
            Charset charset = Charsets.UTF_8;

            String configContents =
                    Resources.toString(Resources.getResource("cassandra.yaml"), charset);

            Files.write(configContents, configFile, charset);
        } catch (IOException e) {
            throw new ToolException("Could not load cassandra.yaml from resources.");
        }

        return configFile;
    }

    private void configureLoggers() {
        Logger cassandraLogger = (Logger) LoggerFactory.getLogger("org.apache.cassandra");
        cassandraLogger.setLevel(Level.ERROR);

        Logger nettyLogger = (Logger) LoggerFactory.getLogger("io.netty");
        nettyLogger.setLevel(Level.ERROR);

        Logger datastaxDriverLogger = (Logger) LoggerFactory.getLogger("com.datastax.driver");
        datastaxDriverLogger.setLevel(Level.ERROR);

        // TODO: This one won't shut up.
        org.apache.log4j.Logger.getLogger("Sigar").setLevel(org.apache.log4j.Level.ERROR);
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
                    "  tool.verbose - Set to anything to have full output\n" +
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
