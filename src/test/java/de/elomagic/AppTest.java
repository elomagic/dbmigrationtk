package de.elomagic;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for simple App.
 */
public class AppTest {

    @Test
    public void testAppStart() {
        App.main(new String[] {"C:\\projects\\ris-unloaded-example\\reload.sql"});

        assertTrue( true );
    }

}
