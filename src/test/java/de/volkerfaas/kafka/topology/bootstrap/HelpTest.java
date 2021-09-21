package de.volkerfaas.kafka.topology.bootstrap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("In the class Help")
public class HelpTest {

    @Nested
    @DisplayName("the method handleArgumentHelp")
    class HandleArgumentHelp {

        @Test
        @DisplayName("should return false when option help is not given")
        void testHandleArgumentHelpNotGiven() {
            final String[] args = new String[] { };
            final boolean result = Help.init(args).handleArgumentHelp().result();
            assertFalse(result);
        }

        @Test
        @DisplayName("should return true when option help is given")
        void testHandleArgumentHelp() {
            final String[] args = new String[] { "--help" };
            final boolean result = Help.init(args).handleArgumentHelp().result();
            assertTrue(result);
        }

    }

}
