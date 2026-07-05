package io.github.borkorajkovic.kafkactl.output;

import java.io.PrintStream;

/**
 * A minimal single-line console progress bar, rendered to {@link System#err}
 * by default so it never interferes with {@code --output json} on stdout.
 */
public final class ProgressBar {

    private static final int WIDTH = 30;

    private final int total;
    private final PrintStream out;

    public ProgressBar(int total) {
        this(total, System.err);
    }

    public ProgressBar(int total, PrintStream out) {
        this.total = total;
        this.out = out;
    }

    public void update(int current) {
        out.print('\r' + render(current, total));
        out.flush();
    }

    public void complete() {
        update(total);
        out.println();
    }

    static String render(int current, int total) {
        if (total <= 0) {
            return "[" + "=".repeat(WIDTH) + "] 100% (0/0)";
        }

        int clamped = Math.max(0, Math.min(current, total));
        int filled = (int) ((clamped / (double) total) * WIDTH);
        int percent = (int) ((clamped / (double) total) * 100);

        StringBuilder bar = new StringBuilder("[");
        bar.append("=".repeat(filled));
        if (filled < WIDTH) {
            bar.append(">").append(" ".repeat(WIDTH - filled - 1));
        }
        bar.append("] ")
                .append(percent).append("% (")
                .append(clamped).append("/").append(total).append(")");

        return bar.toString();
    }
}
