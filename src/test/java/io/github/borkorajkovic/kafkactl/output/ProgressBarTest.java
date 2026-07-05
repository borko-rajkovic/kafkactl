package io.github.borkorajkovic.kafkactl.output;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProgressBarTest {

    @Test
    void rendersZeroPercentAtStart() {
        assertEquals("[>                             ] 0% (0/10)", ProgressBar.render(0, 10));
    }

    @Test
    void rendersFullBarAtCompletion() {
        assertEquals("[==============================] 100% (10/10)", ProgressBar.render(10, 10));
    }

    @Test
    void rendersPartialProgress() {
        String result = ProgressBar.render(5, 10);
        assertTrue(result.contains("50%"));
        assertTrue(result.contains("(5/10)"));
    }

    @Test
    void clampsCurrentAboveTotal() {
        assertEquals("[==============================] 100% (10/10)", ProgressBar.render(15, 10));
    }

    @Test
    void handlesZeroTotalWithoutDividingByZero() {
        assertEquals("[==============================] 100% (0/0)", ProgressBar.render(0, 0));
    }
}
