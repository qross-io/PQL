DEBUG ON;

-- PRINT $x(2, $p(2, 4));
-- CALL $x(2, $p(2, 4));

PRINT """hello'''""";

RETURN $x(4);

FUNCTION $x($a, $b DEFAULT 3)
    BEGIN
        RETURN $a * $b;
    END;

FUNCTION $p($a, $b)
    BEGIN
        RETURN $a + $b;
    END;