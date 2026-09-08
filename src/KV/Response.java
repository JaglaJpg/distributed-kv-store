package KV;

public class Response {
    private final ExecutionStatus status;
    private final String payload; // Will be null for PUT/DELETE, filled for GET

    // Constructor for statuses (PUT/DELETE)
    public Response(ExecutionStatus status) {
        this.status = status;
        this.payload = null;
    }

    // Constructor for data data (GET)
    public Response(ExecutionStatus status, String payload) {
        this.status = status;
        this.payload = payload;
    }

    // A helper method for your network thread to print the final network string
    public String toNetworkString() {
        if (payload != null) {
            return payload; // Just send the data for a successful GET
        }
        return status.name(); // Send "OK_CREATED", "ERR_NOT_FOUND", etc.
    }

}
