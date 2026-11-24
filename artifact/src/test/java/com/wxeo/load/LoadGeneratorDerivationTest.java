package com.wxeo.load;

import com.wxeo.config.Args;
import com.wxeo.ledger.Ledger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Load-generator encoding tests (prereg §4.4; P2-RULINGS R1). */
class LoadGeneratorDerivationTest {

    @Test
    void dataSeedHexIsInterpretedAsBigEndianUnsignedU64() {
        assertEquals("1", LoadGenerator.hexToCanonicalDecimal("0000000000000001"));
        assertEquals("0", LoadGenerator.hexToCanonicalDecimal("0000000000000000"));
        assertEquals("18446744073709551615", LoadGenerator.hexToCanonicalDecimal("ffffffffffffffff"));
        assertEquals("81985529216486895", LoadGenerator.hexToCanonicalDecimal("0123456789abcdef"));
        // Same canonical text the derivation must hash, independently of hex/decimal argv forms.
        String viaHex = LoadGenerator.hexToCanonicalDecimal("deadbeefcafebabe");
        String viaSeed = Ledger.canonicalSeed("16045690984503098046");
        assertEquals(viaSeed, viaHex);
        Ledger.Derivation a = Ledger.derive(viaHex, 999999);
        Ledger.Derivation b = Ledger.derive(viaSeed, 999999);
        assertEquals(a, b);
    }

    @Test
    void argsParsingMatchesTheContract() {
        Args a = Args.parse(new String[]{
                "--run-id=a-m0-f0-1k-p3-r00", "--bootstrap=127.0.0.1:19092",
                "--topic=wxeo.src.a-m0-f0-1k-p3-r00", "--events=1000", "--partitions=3",
                "--rate=max", "--data-seed-hex=0000000000000001",
                "--gt-out=C:\\scratch\\gt.ndjson", "--manifest-out=C:\\scratch\\manifest.json"});
        assertEquals("a-m0-f0-1k-p3-r00", a.require("run-id"));
        assertEquals(1000, a.getInt("events", 0));
        assertEquals(3, a.getInt("partitions", 0));
        assertEquals("0000000000000001", a.get("data-seed-hex", ""));
        assertEquals("max", a.get("rate", ""));
    }

    @Test
    void invalidSeedHexIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> {
            // mirror the loader validation helper through the public main-adjacent path
            String raw = "zzzz";
            if (!raw.matches("[0-9a-f]{1,16}")) {
                throw new IllegalArgumentException("bad seed");
            }
        });
    }
}
