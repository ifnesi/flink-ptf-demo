package io.confluent.demo.ptf;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.Instant;

import static org.apache.flink.table.annotation.ArgumentTrait.REQUIRE_ON_TIME;
import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

/**
 * Per-user click inactivity summary.
 *
 * Maintains a per-user (partition) total click count.
 * After the configured timeout of event-time inactivity, emits the total click count, then clears state.
 */
public class ClickInactivitySummary extends ProcessTableFunction<ClickInactivitySummary.Summary> {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    public static class Summary {
        public String user_id;
        public int total_clicks;
    }

    public static class ClickState {
        public String userId = "";
        public int totalClicks = 0;
    }

    public void eval(
            Context ctx,
            @StateHint ClickState state,
            @ArgumentHint({SET_SEMANTIC_TABLE, REQUIRE_ON_TIME}) Row input) {

        state.userId = input.getFieldAs("user_id");

        // Increment the total click count
        state.totalClicks++;

        // Register or replace a named timer; this resets the inactivity clock
        TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
        timeCtx.registerOnTime("inactivity", timeCtx.time().plus(TIMEOUT));
    }

    public void onTimer(OnTimerContext ctx, ClickState state) {
        // Timer fired — no new click arrived within the timeout
        Summary summary = new Summary();
        summary.user_id = state.userId;
        summary.total_clicks = state.totalClicks;
        collect(summary);

        // Reset the partition so a returning user starts a new inactivity window instead of
        // retaining the click counter indefinitely. The fired timer is already consumed, so
        // only state needs clearing.
        ctx.clearAllState();
    }
}
