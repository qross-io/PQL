DEBUG ON;

SET $C := "0 0 * * * ? *";
SET $T := @NOW;
SET $RECENT := [];
SET $M := $C TICK BY $T;
FOR $I IN 1 TO 10 LOOP
    IF $M IS NOT NULL THEN
        LET $RECENT ADD ${ $M FORMAT 'yyyy-MM-dd HH:mm:ss' };
        SET $T := $M PLUS 1 SECOND;
        SET $M := $C TICK BY $T;
    END IF;
END LOOP;
OUTPUT $RECENT;

EXEC 'PRINT 123';

VAR $a := IF 2 > 1 THEN 'hello' ELSE 'world' END;
SET $b := CASE $a WHEN 'hello' THEN 1 ELSE 2 END;

PRINT $a;
PRINT $b;

IF #{filter} IS DEFINED THEN
    PRINT 'HELLO';
END IF;

IF '#{filter}' IS DEFINED THEN
    PRINT 'WORLD';
END IF;
