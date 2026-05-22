package io.confluent.demo.ptf;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.annotation.ArgumentTrait.REQUIRE_ON_TIME;
import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

/**
 * Per-user click inactivity summary.
 *
 * Maintains a per-user (partition) map of product_id -> (product_name, count).
 * After {@code timeoutSecs} of event-time inactivity, emits one row containing
 * the detection timestamp and the full list of product counts, then clears state.
 *
 * Output shape (from {@code collect}): ROW&lt;detected_at TIMESTAMP_LTZ(3),
 *                                          click_counts ARRAY&lt;ROW&lt;product_id STRING,
 *                                                                  product_name STRING,
 *                                                                  count INT&gt;&gt;&gt;
 *
 * Framework auto-prepends the partition key (user_id) and the row event time,
 * so the SQL-visible output is (user_id, $rowtime, detected_at, click_counts).
 */

@DataTypeHint("ROW<detected_at TIMESTAMP_LTZ(3), click_counts ARRAY<ROW<product_id STRING, product_name STRING, count INT>>>")
public class ClickInactivitySummary extends ProcessTableFunction<Row> {

    /** Per-user managed state. */
    public static class ClickState {
        public Map<String, ProductCount> counts = new HashMap<>();
    }

    /** State value held per product_id. */
    public static class ProductCount {
        public String productName;
        public int count;
    }

    public void eval(
            Context ctx,
            @StateHint ClickState state,
            @ArgumentHint({SET_SEMANTIC_TABLE, REQUIRE_ON_TIME}) Row input,
            int timeoutSecs) {

        String productId   = input.getFieldAs("product_id");
        String productName = input.getFieldAs("product_name");

        ProductCount pc = state.counts.computeIfAbsent(productId, k -> new ProductCount());
        pc.productName = productName;
        pc.count++;

        // Re-register the SAME named timer on every click; this resets the inactivity clock.
        TimeContext<Instant> t = ctx.timeContext(Instant.class);
        t.registerOnTime("inactivity", t.time().plus(Duration.ofSeconds(timeoutSecs)));
    }

    public void onTimer(OnTimerContext ctx, ClickState state) {
        TimeContext<Instant> t = ctx.timeContext(Instant.class);

        Row[] items = state.counts.entrySet().stream()
                .map(e -> Row.of(e.getKey(), e.getValue().productName, e.getValue().count))
                .toArray(Row[]::new);

        collect(Row.of(t.time(), items));

        // Reset the partition so a returning user starts a new inactivity window instead of
        // retaining the click counter indefinitely. The fired timer is already consumed, so
        // only state needs clearing.
        ctx.clearAllState();
    }
}
