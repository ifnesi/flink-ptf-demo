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
 * Process Table Function (PTF) for detecting user click inactivity.
 *
 * This PTF demonstrates how to:
 * 1. Maintain per-user state (click counts per product)
 * 2. Schedule event-time timers to detect inactivity
 * 3. Emit summary events when users stop clicking
 *
 * Key Concepts:
 * - Partitioned by user_id (each user has independent state)
 * - Uses event-time semantics (based on click_ts, not wall-clock time)
 * - Fires a timer after N seconds of inactivity (no new clicks)
 * - Clears state after emitting to start fresh for next activity burst
 */
public class ClickInactivitySummary
        extends ProcessTableFunction<ClickInactivitySummary.FlinkOutputTable> {

    /**
     * Output schema: Defines the structure of rows emitted to the sink table.
     * This POJO maps to the Flink SQL output table columns.
     *
     * Each field becomes a column in the output table (user-clicks-summary).
     */
    public static class FlinkOutputTable {
        public String user_id;        // Which user this summary is for
        public Instant detected_at;   // When inactivity was detected (event-time)
        public String clicks_summary; // Aggregated click counts (e.g., "Pizza:3, Burger:1")
    }

    /**
     * Managed state: Flink automatically checkpoints and restores this per partition (user_id).
     * This is the "memory" of the PTF for each user.
     *
     * State is scoped to the partition key (user_id), so each user has their own independent state.
     * Flink handles fault tolerance—if the job restarts, state is restored from the last checkpoint.
     */
    public static class StateStore {
        public String userId = "";                                    // Current user being tracked
        public Map<String, Integer> productClicks = new HashMap<>();  // {Product_name: click_count}
        public Instant detectedAt = null;                             // When the inactivity timer will fire
    }

    /**
     * eval() is called for EACH input row (click event).
     * This is the "reactive" part—it processes incoming events.
     *
     * @param ctx         Flink context for timers, state, and output
     * @param state       Per-user managed state (automatically restored on restart)
     * @param input       The incoming row from the source table (user-clicks)
     * @param timeoutSeconds How long to wait for inactivity before firing the timer
     *
     * Annotations explained:
     * - @StateHint: Tells Flink this parameter holds managed state (checkpointed automatically)
     * - @ArgumentHint(SET_SEMANTIC_TABLE): Input is a table partitioned by user_id
     * - @ArgumentHint(REQUIRE_ON_TIME): Enables event-time semantics (uses $rowtime from input)
     */
    public void eval(
            Context ctx,
            @StateHint StateStore state,
            @ArgumentHint({SET_SEMANTIC_TABLE, REQUIRE_ON_TIME}) Row input,
            int timeoutSeconds
        ) {

        // Extract fields from the incoming click event
        String userId = input.getFieldAs("user_id");
        String productName = input.getFieldAs("product_name");

        // Filter out heartbeat messages (used to advance watermarks in low-traffic scenarios)
        // Heartbeats were designed to have null user_id and product_name, they only carry a timestamp
        if (userId == null || productName == null) {
            return;
        }

        // Update state: Store the user ID
        state.userId = userId;
        
        // Update state: Increment the click count for this product
        // If the product hasn't been clicked yet, default to 0 and add 1
        state.productClicks.put(
            productName,
            state.productClicks.getOrDefault(productName, 0) + 1
        );

        // Schedule (or reschedule) the inactivity timer
        // This is the key to detecting "absence of events"
        TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
        Instant currentTime = timeCtx.time();  // Current event time (from $rowtime)
        if (currentTime != null) {
            // Calculate when the timer should fire (current event time + timeout)
            Instant detectedAt = currentTime.plus(Duration.ofSeconds(timeoutSeconds));
            state.detectedAt = detectedAt;
            
            // Register a named timer. Using the same name ("inactivity") overwrites any previous timer.
            // This means each new click "resets" the inactivity countdown.
            // The timer will fire when the watermark passes 'detectedAt' (i.e., no new events for N seconds)
            timeCtx.registerOnTime("inactivity", detectedAt);
        }
    }

    /**
     * onTimer() is called when a registered timer fires.
     * This is the "proactive" part—it generates events from the ABSENCE of input.
     *
     * The timer fires when:
     * - The watermark advances past the scheduled time (detectedAt)
     * - No new clicks arrived to reset the timer
     *
     * This is how PTFs detect inactivity: the timer represents "N seconds of silence."
     *
     * @param ctx   Context for clearing state and accessing timer metadata
     * @param state The current state for this partition (user)
     */
    public void onTimer(OnTimerContext ctx, StateStore state) {
        // Safety check: If state is empty or invalid, just clear and return
        // This shouldn't happen in normal operation, but guards against edge cases
        if (state.userId == null || state.userId.isEmpty() || state.productClicks.isEmpty() || state.detectedAt == null) {
            ctx.clearAllState();
            return;
        }

        // Build the output row to emit to the sink table
        FlinkOutputTable summary = new FlinkOutputTable();
        summary.user_id = state.userId;
        summary.detected_at = state.detectedAt;  // When inactivity was detected
        
        // Format the click counts as a human-readable string
        // Example: "Burger:1, Pizza:3, Sushi:2"
        summary.clicks_summary = state.productClicks.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())  // Sort alphabetically by product name
            .map(e -> e.getKey() + ":" + e.getValue())
            .collect(Collectors.joining(", "));
        
        // Emit the summary row to the output table (`user-clicks-summary`)
        collect(summary);

        // Clear all state for this partition (user)
        // This ensures the next burst of clicks starts fresh with a clean slate.
        // Without this, the click counts would accumulate indefinitely across sessions.
        // The timer itself is already consumed (fired), so we only need to clear state.
        ctx.clearAllState();
    }
}