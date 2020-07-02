package edu.upenn.zootester.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {

    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    private String scenario = "divergence-2";
    private int threads = 1;
    private int executions = 1;
    private int phases = 3;
    private int faults = 0;
    private int requests = 0;
    private int basePort = 11221;
    private Long seed = null;
    private int harnesses = 1;

    private Config() {
    }

    public String getScenario() {
        return scenario;
    }

    public int getThreads() {
        return threads;
    }

    public int getExecutions() {
        return executions;
    }

    public int getPhases() {
        return phases;
    }

    public int getFaults() {
        return faults;
    }

    public int getRequests() {
        return requests;
    }

    public int getBasePort() {
        return basePort;
    }

    public boolean hasSeed() {
        return seed != null;
    }

    public long getSeed() {
        return seed;
    }

    public int getHarnesses() {
        return harnesses;
    }

    public static Config parseArgs(final String[] args) throws ConfigException {
        try {
            final Config config = new Config();
            for (int i = 0; i < args.length; ++i) {
                switch (args[i]) {
                    case "--scenario":
                    case "-s":
                        config.scenario = args[++i];
                        break;
                    case "--threads":
                    case "-t":
                        config.threads = Integer.parseInt(args[++i]);
                        break;
                    case "--executions":
                    case "-e":
                        config.executions = Integer.parseInt(args[++i]);
                        break;
                    case "--phases":
                    case "-p":
                        config.phases = Integer.parseInt(args[++i]);
                        break;
                    case "--faults":
                    case "-f":
                        config.faults = Integer.parseInt(args[++i]);
                        break;
                    case "--requests":
                    case "-r":
                        config.requests = Integer.parseInt(args[++i]);
                        break;
                    case "--basePort":
                        config.basePort = Integer.parseInt(args[++i]);
                        break;
                    case "--seed":
                        config.seed = Long.parseLong(args[++i]);
                        break;
                    case "--harnesses":
                    case "-h":
                        config.harnesses = Integer.parseInt(args[++i]);
                        break;
                    default:
                        throw new Exception("Unrecognized argument " + args[i]);
                }
            }
            return config;
        } catch (final Exception e) {
            LOG.error("Error while parsing the arguments", e);
            throw new ConfigException(e);
        }
    }

    public static void showUsage() {
        System.out.println("For a list of possible parameters, see the class " + Config.class.getCanonicalName());
    }
}
