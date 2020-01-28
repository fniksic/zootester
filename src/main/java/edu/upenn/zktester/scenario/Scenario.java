package edu.upenn.zktester.scenario;

import java.io.IOException;

public interface Scenario {

    void init() throws IOException;

    void execute() throws Exception;
}
