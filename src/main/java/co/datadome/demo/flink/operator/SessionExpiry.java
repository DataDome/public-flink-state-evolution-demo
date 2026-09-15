package co.datadome.demo.flink.operator;

import java.time.Duration;

/**
 * How long a session is kept without activity.
 *
 * <p>This is a constant rather than a job parameter: it exists so that the job does not leak state
 * forever, not as something to tune. At 24 hours it will not fire during a demo, and that is fine.
 */
public final class SessionExpiry {

    /** Inactivity period after which all state for an IP address is discarded. */
    public static final Duration GAP = Duration.ofHours(24);

    public static final long GAP_MS = GAP.toMillis();

    private SessionExpiry() {}
}
