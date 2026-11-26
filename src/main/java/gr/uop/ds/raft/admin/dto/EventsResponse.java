package gr.uop.ds.raft.admin.dto;

import java.util.List;

import gr.uop.ds.raft.NodeLogger;

 /** GET /events: startedAt changes when the node restarts, so the page knows that its cursor is no longer valid. */
public record EventsResponse(long startedAt, List<NodeLogger.Event> events) { }