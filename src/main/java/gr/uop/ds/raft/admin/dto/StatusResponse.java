package gr.uop.ds.raft.admin.dto;

import gr.uop.ds.raft.NodeStatus;

 /** GET /status: the snapshot of the node plus what only the adapter knows (the active faults). */
public record StatusResponse(long startedAt, NodeStatus status, String faults) { }
