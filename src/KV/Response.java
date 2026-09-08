package kv;

public class Response {
    private final ExecutionStatus status;
    private final String payload; // Will be null for PUT/DELETE, filled for GET

    // Constructor for statuses (PUT/DELETE)
    public Response(ExecutionStatus status) {
        this.status = status;
        this.payload = null;
    }

    // Constructor for data (GET)
    public Response(ExecutionStatus status, String payload) {
        this.status = status;
        this.payload = payload;
    }

    public ExecutionStatus getStatus() {
    	return this.status;
    }

    public String getPayload() {
    	return this.payload;
    }

}
