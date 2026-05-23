package io.confluent.demo.ptf;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.annotation.ArgumentTrait.REQUIRE_ON_TIME;
import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

/**
 * Per-user click inactivity summary.
 *
 * For each partition (user_id), count clicks.
 * When no new click arrives within timeoutSeconds of event-time inactivity,
 * emit one summary row and clear state.
 */
public class ClickInactivitySummary
        extends ProcessTableFunction<ClickInactivitySummary.Summary> {

    public static class Summary {
        public String user_id;
        public Instant detected_at;
        public String clicks_summary;
    }

    public static class ClickState {
        public String userId = "";
        public Map<String, Integer> productClicks = new HashMap<>();
        public Instant detectedAt = null;
    }

    public void eval(
            Context ctx,
            @StateHint ClickState state,
            @ArgumentHint({SET_SEMANTIC_TABLE, REQUIRE_ON_TIME}) Row input,
            int timeoutSeconds
        ) {

        String userId = input.getFieldAs("user_id");
        String productName = input.getFieldAs("product_name");

        // Ignore heartbeat messages
        if (userId == null || productName == null) {
            return;
        }

        state.userId = userId;
        
        // Track clicks per product
        state.productClicks.put(
            productName,
            state.productClicks.getOrDefault(productName, 0) + 1
        );

        TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
        Instant currentTime = timeCtx.time();
        if (currentTime != null) {
            Instant detectedAt = currentTime.plus(Duration.ofSeconds(timeoutSeconds));
            state.detectedAt = detectedAt;
            timeCtx.registerOnTime("inactivity", detectedAt);
        }
    }

    public void onTimer(OnTimerContext ctx, ClickState state) {
        if (state.userId == null || state.userId.isEmpty() || state.productClicks.isEmpty() || state.detectedAt == null) {
            ctx.clearAllState();
            return;
        }

        Summary summary = new Summary();
        summary.user_id = state.userId;
        summary.detected_at = state.detectedAt;
        
        // Format as "Product1:Count1, Product2:Count2, ..."
        summary.clicks_summary = state.productClicks.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(e -> e.getKey() + ":" + e.getValue())
            .collect(Collectors.joining(", "));
        
        collect(summary);

        // Reset the partition so a returning user starts a new inactivity window instead of
        // retaining the click counter indefinitely. The fired timer is already consumed, so
        // only state needs clearing.
        ctx.clearAllState();
    }
}