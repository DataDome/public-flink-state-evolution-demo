package co.datadome.demo.flink.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class IpStatsTest {

    private static HttpRequest request(String path, int statusCode) {
        return new HttpRequest(1_000, "10.0.0.1", path, "curl/8", statusCode);
    }

    @Test
    void errorRatioIsZeroWithoutAnyRequest() {
        assertThat(new IpStats().getErrorRatio()).isZero();
    }

    @Test
    void errorRatioCountsFailuresOverTheTotal() {
        IpStats stats = IpStats.startingWith(request("/a", 200));
        stats.accumulate(request("/a", 200));
        stats.accumulate(request("/a", 403));
        stats.accumulate(request("/a", 500));
        stats.accumulate(request("/a", 200));

        assertThat(stats.getTotalCount()).isEqualTo(4);
        assertThat(stats.getErrorCount()).isEqualTo(2);
        assertThat(stats.getErrorRatio()).isEqualTo(0.5);
    }

    @Test
    void errorRatioDoesNotDriftAsTheSessionGrows() {
        IpStats stats = IpStats.startingWith(request("/a", 200));
        for (int i = 0; i < 1_000; i++) {
            // A steady one-in-ten failure rate.
            stats.accumulate(request("/a", i % 10 == 0 ? 500 : 200));
        }

        assertThat(stats.getErrorRatio()).isCloseTo(0.1, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    void aCopyIsIndependentOfTheOriginal() {
        IpStats stats = IpStats.startingWith(request("/a", 200));
        stats.accumulate(request("/a", 200));

        IpStats snapshot = stats.copy();
        stats.accumulate(request("/a", 500));

        assertThat(snapshot.getTotalCount()).isEqualTo(1);
        assertThat(stats.getTotalCount()).isEqualTo(2);
    }
}
