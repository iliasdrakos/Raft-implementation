package gr.uop.ds.raft.admin.dto;

/** POST /command answer: the 4 fields of the gRPC CommandResponse as a plain record (protobuf objects are not JSON friendly). */
public record CommandResult(boolean success, String result, String leaderId, long index) { }
