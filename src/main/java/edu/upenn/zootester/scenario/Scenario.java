package edu.upenn.zootester.scenario;

import edu.upenn.zootester.util.Config;

import java.io.IOException;

public interface Scenario {

    void init(Config config) throws IOException;

    void execute() throws Exception;
}
