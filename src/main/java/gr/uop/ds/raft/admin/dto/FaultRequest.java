package gr.uop.ds.raft.admin.dto;

/** POST /fault body, e.g. {"type":"drop","value":30}. value is an Integer, so a missing value is null and not 0. */
public record FaultRequest(String type, Integer value) {}
