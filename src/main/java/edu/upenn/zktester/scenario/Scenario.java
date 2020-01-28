package edu.upenn.zktester.scenario;

import edu.upenn.zktester.util.Config;

import java.io.IOException;

public interface Scenario {

    void init(Config config) throws IOException;

    void execute() throws Exception;
}
