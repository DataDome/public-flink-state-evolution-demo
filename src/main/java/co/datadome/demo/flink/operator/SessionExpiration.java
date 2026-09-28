package co.datadome.demo.flink.operator;

import java.time.Duration;

/**
 * Constants to handle session expiration.
 */
public final class SessionExpiration {

    /**
     * Inactivity period after which all state for an IP address is discarded.
     */
    public static final Duration GAP = Duration.ofHours(24);

    public static final long GAP_MS = GAP.toMillis();

    private SessionExpiration() {
    }
}
