package com.github.yarosla.httpstorage;

import com.beust.jcommander.Parameter;

@SuppressWarnings({"WeakerAccess", "FieldCanBeLocal", "unused"})
public class Parameters {

    @Parameter(names = {"--debug", "-v"}, description = "Show debug log")
    private boolean debug;

    @SuppressWarnings("FieldCanBeLocal")
    @Parameter(names = {"--limit", "-l"}, description = "Memory limit in megabytes")
    private long memoryLimit = 1000;

    @Parameter(names = {"--host", "-H"}, description = "Set http host to listen on")
    private String host = "0.0.0.0";

    @Parameter(names = {"--port", "-P"}, description = "Set http port to listen to")
    private int port = 8080;

    @Parameter(names = {"--static", "-s"}, description = "Serve static content from this directory")
    private String staticResourcesPath;

    @Parameter(names = {"--cors", "-c"}, description = "Allow cross-origin requests")
    private boolean corsEnabled;

    @Parameter(names = {"--help", "-h"}, help = true, description = "Display help")
    private boolean help;

    public boolean isDebug() {
        return debug;
    }

    public long getMemoryLimit() {
        return memoryLimit * 1_000_000; // convert megabytes to bytes
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getStaticResourcesPath() {
        return staticResourcesPath;
    }

    public boolean isCorsEnabled() {
        return corsEnabled;
    }

    public boolean isHelp() {
        return help;
    }
}
