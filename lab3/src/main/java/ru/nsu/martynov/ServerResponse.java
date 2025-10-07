package ru.nsu.martynov;

import java.util.List;

public class ServerResponse {
    private String message;
    private List<String> successors;

    public ServerResponse() {}

    public String getMessage() { return message; }

    public List<String> getSuccessors() { return successors; }
}
